from copy import deepcopy
from typing import Any, Dict, List, Optional, Union

from .adaptor import DatabaseAdapter, MySqlAdapter, PostgresAdapter
from .builders import DeleteBuilder, InsertBuilder, SelectBuilder, UpdateBuilder
from .enum import JoinType
from .exceptions import DoesNotExist, MultipleObjectsReturned
from .express import Expression
from .expressions import ExpressionHandler
from .F import F
from .field import ForeignKeyField
from .parser import ResultParser
from .Q import Q
from .sqlrustler import DatabaseType, get_db_type_with_alias


class QuerySet:
    def __init__(self, model, alias: str = "default"):
        self.model = model
        self.alias = alias
        self.state = {
            "select": [f"{model.table_name()}.{field}" for field in model._fields],
            "where": [],
            "order_by": [],
            "limit": None,
            "offset": None,
            "joins": [],
            "group_by": [],
            "having": [],
            "with": [],
            "window": [],
            "annotations": set(),
            "distinct": False,
            "distinct_on": [],
            "for_update": False,
            "for_share": False,
            "nowait": False,
            "skip_locked": False,
            "for_update_of": [],
            "no_key": False,
        }
        self.params: List[Any] = []
        self._param_counter = 1
        self._selected_related = set()
        self._prefetch_related = set()
        self._related_joins = set()
        self._raw_results = False  # Flag for raw results
        self._adapter = self._get_adapter()
        self._builder = SelectBuilder(self)
        self._parser = ResultParser(self)
        self._expression_handler = ExpressionHandler(self)

    def _get_adapter(self) -> DatabaseAdapter:
        db_type = get_db_type_with_alias(self.alias)
        if db_type == DatabaseType.Postgres:
            return PostgresAdapter()
        elif db_type == DatabaseType.MySql:
            return MySqlAdapter()
        raise ValueError(f"Unsupported database type: {db_type}")

    def clone(self) -> "QuerySet":
        new_qs = QuerySet(self.model, self.alias)
        new_qs.state = deepcopy(self.state)
        new_qs.params = self.params[:]
        new_qs._param_counter = self._param_counter
        new_qs._selected_related = self._selected_related.copy()
        new_qs._prefetch_related = self._prefetch_related.copy()
        new_qs._related_joins = self._related_joins.copy()
        new_qs._raw_results = self._raw_results
        return new_qs

    def raw(self) -> "QuerySet":
        """Return raw dictionaries instead of Model instances."""
        qs = self.clone()
        qs._raw_results = True
        return qs

    def select(self, *fields, distinct: bool = False) -> "QuerySet":
        qs = self.clone()
        qs.state["select"] = [
            f"{self.model.table_name()}.{field}" if field != "*" else field
            for field in fields
        ]
        qs.state["distinct"] = distinct
        qs._raw_results = True  # Custom select implies raw results
        return qs

    def values(self, *fields) -> "QuerySet":
        """Return results as dictionaries for specified fields."""
        return self.select(*fields).raw()

    def values_list(self, *fields, flat: bool = False) -> "QuerySet":
        """Return results as lists or single values if flat=True."""
        if flat and len(fields) > 1:
            raise ValueError("flat=True is invalid with multiple fields")
        qs = self.select(*fields).raw()
        if flat:
            qs._raw_results = True
            qs._flat = True
        return qs

    def where(self, *args, **kwargs) -> "QuerySet":
        qs = self.clone()
        for arg in args:
            if isinstance(arg, Q):
                sql, params = self._expression_handler.process_q_object(arg)
                if sql:
                    qs.state["where"].append(sql)
                    qs.params.extend(params)
            elif isinstance(arg, Expression):
                qs.state["where"].append(arg.sql)
                qs.params.extend(arg.params)
            elif isinstance(arg, F):
                sql, params = self._expression_handler.process_f_object(arg)
                qs.state["where"].append(sql)
                qs.params.extend(params)
            else:
                qs.state["where"].append(str(arg))
        if kwargs:
            q = Q(**kwargs)
            sql, params = self._expression_handler.process_q_object(q)
            if sql:
                qs.state["where"].append(sql)
                qs.params.extend(params)
        return qs

    def filter(self, *args, **kwargs) -> "QuerySet":
        return self.where(*args, **kwargs)

    def exclude(self, *args, **kwargs) -> "QuerySet":
        qs = self.clone()
        q = Q(*args, **kwargs)
        q = ~q
        sql, params = self._expression_handler.process_q_object(q)
        if sql:
            qs.state["where"].append(sql)
            qs.params.extend(params)
        return qs

    def annotate(self, **annotations) -> "QuerySet":
        qs = self.clone()
        select_parts = []
        has_window_function = False

        for alias, expression in annotations.items():
            qs.state["annotations"].add(alias)
            if isinstance(expression, F):
                if "OVER" in expression.field:
                    has_window_function = True
                select_parts.append(f"{expression.field} AS {alias}")
            elif isinstance(expression, Expression):
                if "OVER" in expression.sql:
                    has_window_function = True
                select_parts.append(f"({expression.sql}) AS {alias}")
                qs.params.extend(expression.params)
            else:
                select_parts.append(f"{expression} AS {alias}")

        qs.state["select"].extend(select_parts)
        if has_window_function and not qs.state["window"]:
            qs.state["window"].append("default_window AS ()")
            for i, select_part in enumerate(qs.state["select"]):
                if "OVER" in select_part and "OVER default_window" not in select_part:
                    qs.state["select"][i] = select_part.replace(
                        "OVER ()", "OVER default_window"
                    )
        return qs

    def select_related(self, *fields) -> "QuerySet":
        qs = self.clone()
        for field in fields:
            if field in qs.model._fields and isinstance(
                qs.model._fields[field], ForeignKeyField
            ):
                qs._selected_related.add(field)
                related_model = qs.model._fields[field].to_model
                join_key = f"{field}__{related_model.table_name()}"
                join_exists = any(
                    related_model.table_name() in join for join in qs.state["joins"]
                )
                if not join_exists:
                    qs.state["joins"].append(
                        f"LEFT JOIN {related_model.table_name()} ON {qs.model.table_name()}.{field} = {related_model.table_name()}.{qs.model._fields[field].related_field}"
                    )
                    qs._related_joins.add(join_key)
                qs.state["select"].extend(
                    [
                        f"{related_model.table_name()}.{related_field} AS {related_model.table_name()}__{related_field}"
                        for related_field in related_model._fields
                    ]
                )
        if "*" in qs.state["select"]:
            qs.state["select"] = [
                f"{qs.model.table_name()}.{field}" for field in qs.model._fields
            ] + [s for s in qs.state["select"] if s != "*"]
        return qs

    def prefetch_related(self, *lookups) -> "QuerySet":
        qs = self.clone()
        qs._prefetch_related.update(lookups)
        return qs

    def join(
        self,
        table: Any,
        on: Union[str, Expression, F],
        join_type: Union[str, JoinType] = JoinType.INNER,
    ) -> "QuerySet":
        qs = self.clone()
        join_type = join_type.value if isinstance(join_type, JoinType) else join_type
        joined_table = table.table_name() if hasattr(table, "table_name") else table
        if isinstance(on, Expression):
            qs.state["joins"].append(f"{join_type} {joined_table} ON {on.sql}")
            qs.params.extend(on.params)
        elif isinstance(on, F):
            qs.state["joins"].append(f"{join_type} {joined_table} ON {on.field}")
            qs.params.extend(on.params)
        else:
            qs.state["joins"].append(f"{join_type} {joined_table} ON {on}")
        return qs

    def group_by(self, *fields) -> "QuerySet":
        qs = self.clone()
        qs.state["group_by"] = [
            f"{self.model.table_name()}.{field}"
            if not isinstance(field, (F, Expression))
            else field.field
            if isinstance(field, F)
            else field.sql
            for field in fields
        ]
        return qs

    def having(self, *conditions) -> "QuerySet":
        qs = self.clone()
        qs.state["having"] = [
            condition.sql if isinstance(condition, Expression) else str(condition)
            for condition in conditions
        ]
        if any(isinstance(c, Expression) for c in conditions):
            qs.params.extend(
                [p for c in conditions if isinstance(c, Expression) for p in c.params]
            )
        return qs

    def window(
        self, alias: str, partition_by: List = None, order_by: List = None
    ) -> "QuerySet":
        qs = self.clone()
        parts = [f"{alias} AS ("]
        if partition_by:
            parts.append(f"PARTITION BY {', '.join(partition_by)}")
        if order_by:
            parts.append(f"ORDER BY {', '.join(order_by)}")
        parts.append(")")
        qs.state["window"].append(" ".join(parts))
        return qs

    def order_by(self, *fields) -> "QuerySet":
        qs = self.clone()
        qs.state["order_by"] = [
            f"{self.model.table_name()}.{field[1:]} DESC"
            if field.startswith("-")
            else f"{self.model.table_name()}.{field} ASC"
            if not isinstance(field, (F, Expression))
            else field.field
            if isinstance(field, F)
            else field.sql
            for field in fields
        ]
        if any(isinstance(f, Expression) for f in fields):
            qs.params.extend(
                [p for f in fields if isinstance(f, Expression) for p in f.params]
            )
        return qs

    def limit(self, limit: int) -> "QuerySet":
        qs = self.clone()
        qs.state["limit"] = limit
        return qs

    def offset(self, offset: int) -> "QuerySet":
        qs = self.clone()
        qs.state["offset"] = offset
        return qs

    def get(self) -> Any:
        qs = self.limit(1)
        sql, params = qs.to_sql()
        with self.model.get_session(alias=self.alias) as tx:
            result = tx.fetch_all(sql, params)
        if not result:
            raise DoesNotExist(f"{self.model.__name__} does not exist.")
        if len(result) > 1:
            raise MultipleObjectsReturned(
                f"Expected 1 {self.model.__name__}, got {len(result)}."
            )
        return self._parser.parse_row(result[0], raw=self._raw_results)

    def first(self) -> Optional[Any]:
        qs = self.order_by("id") if not self.state["order_by"] else self
        result = qs.limit(1).execute()
        return result[0] if result else None

    def last(self) -> Optional[Any]:
        qs = self.clone()
        if not qs.state["order_by"]:
            qs = qs.order_by("-id")
        else:
            qs.state["order_by"] = [
                f"-{field}" if not field.startswith("-") else field[1:]
                for field in qs.state["order_by"]
            ]
        result = qs.limit(1).execute()
        return result[0] if result else None

    def none(self) -> "QuerySet":
        qs = self.clone()
        qs.state["where"] = ["1=0"]
        qs.params = []
        return qs

    def all(self) -> "QuerySet":
        return self.clone()

    def distinct(self, *fields) -> "QuerySet":
        qs = self.clone()
        qs.state["distinct"] = True
        qs.state["distinct_on"] = list(fields) if fields else []
        return qs

    def select_for_update(
        self,
        nowait: bool = False,
        skip_locked: bool = False,
        of: Optional[List[str]] = None,
        no_key: bool = False,
    ) -> "QuerySet":
        qs = self.clone()
        qs.state["for_update"] = True
        qs.state["nowait"] = nowait
        qs.state["skip_locked"] = skip_locked
        qs.state["for_update_of"] = of or []
        qs.state["no_key"] = no_key
        return qs

    def aggregate(self, **annotations) -> Dict[str, Any]:
        qs = self.clone()
        qs._raw_results = True  # Aggregates return raw dicts
        select_parts = []
        for alias, expr in annotations.items():
            qs.state["annotations"].add(alias)
            select_parts.append(
                f"{expr.sql} AS {alias}"
                if isinstance(expr, Expression)
                else f"{expr.field} AS {alias}"
            )
            if isinstance(expr, Expression):
                qs.params.extend(expr.params)
        qs.state["select"] = select_parts
        sql, params = qs.to_sql()
        with self.model.get_session(alias=self.alias) as tx:
            result = tx.fetch_all(sql, params)
        return (
            self._parser.parse_aggregate_row(result[0], annotations) if result else {}
        )

    def execute(self) -> List[Any]:
        sql, params = self.to_sql()
        with self.model.get_session(alias=self.alias) as tx:
            result = tx.fetch_all(sql, params)
        instances = [
            self._parser.parse_row(row, raw=self._raw_results) for row in result
        ]
        if self._prefetch_related and not self._raw_results:
            instances = self._parser.handle_prefetch_related(
                instances, self._prefetch_related
            )
        if hasattr(self, "_flat") and self._flat:
            return [list(row.values())[0] for row in instances]
        return instances

    def count(self) -> int:
        qs = self.clone()
        qs.state["select"] = ["COUNT(*) AS count"]
        qs.state["order_by"] = []
        qs._raw_results = True
        sql, params = qs.to_sql()
        with self.model.get_session(alias=self.alias) as tx:
            result = tx.fetch_all(sql, params)
        return result[0]["count"] if result else 0

    def exists(self) -> bool:
        qs = self.clone()
        qs.state["select"] = ["1"]
        qs.state["order_by"] = []
        qs = qs.limit(1)
        qs._raw_results = True
        sql, params = qs.to_sql()
        with self.model.get_session(alias=self.alias) as tx:
            result = tx.fetch_all(sql, params)
        return bool(result)

    def update(self, **kwargs) -> int:
        qs = self.clone()
        qs._builder = UpdateBuilder(qs)
        return qs._builder.execute_update(kwargs)

    def delete(self) -> int:
        qs = self.clone()
        qs._builder = DeleteBuilder(qs)
        return qs._builder.execute_delete()

    def bulk_create(
        self, objs: List[Any], batch_size: Optional[int] = None
    ) -> Optional[int]:
        qs = self.clone()
        qs._builder = InsertBuilder(qs)
        return qs._builder.execute_bulk_create(objs, batch_size)

    def to_sql(self) -> tuple[str, List[Any]]:
        return self._builder.build_sql()

    def explain(
        self,
        analyze: bool = False,
        verbose: bool = False,
        costs: bool = False,
        buffers: bool = False,
        timing: bool = False,
    ) -> List[Dict]:
        options = [
            opt
            for opt, flag in [
                ("ANALYZE", analyze),
                ("VERBOSE", verbose),
                ("COSTS", costs),
                ("BUFFERS", buffers),
                ("TIMING", timing),
            ]
            if flag
        ]
        sql, params = self.to_sql()
        explain_sql = f"EXPLAIN ({' '.join(options)}) {sql}"
        with self.model.get_session(alias=self.alias) as tx:
            result = tx.fetch_all(explain_sql, params)
        return result
