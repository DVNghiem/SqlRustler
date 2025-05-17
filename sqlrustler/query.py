from typing import Any, Dict, List, Tuple, Union, Optional
from abc import ABC, abstractmethod
import decimal
import datetime
import json
from .enum import Operator, JoinType
from .F import F
from .express import Expression
from .adaptor import DatabaseAdapter, PostgresAdapter, MySqlAdapter
from .exceptions import DoesNotExist, MultipleObjectsReturned
from .Q import Q

from .field import ForeignKeyField
from .sqlrustler import get_db_type_with_alias, DatabaseType


class QueryBuilder(ABC):
    """Builder for generating database-specific SQL queries."""
    
    def __init__(self, queryset: 'QuerySet'):
        self.queryset = queryset
        self.adapter = self._get_adapter()

    @abstractmethod
    def _get_adapter(self) -> DatabaseAdapter:
        pass

    def build_sql(self) -> Tuple[str, List]:
        parts = []
        self._build_sql_parts(parts)
        return " ".join(parts), self.queryset.params

    def _build_sql_parts(self, parts: List[str]):
        self.queryset._add_with_clause(parts)
        self.queryset._add_select_clause(parts)
        self.queryset._add_from_clause(parts)
        self.queryset._add_joins_clause(parts)
        self.queryset._add_where_clause(parts)
        self.queryset._add_group_by_clause(parts)
        self.queryset._add_having_clause(parts)
        self.queryset._add_window_clause(parts)
        self.queryset._add_order_by_clause(parts)
        self.queryset._add_limit_clause(parts)
        self.queryset._add_offset_clause(parts)
        self.queryset._add_locking_clauses(parts)


class PostgresQueryBuilder(QueryBuilder):
    def _get_adapter(self) -> DatabaseAdapter:
        return PostgresAdapter()


class MySqlQueryBuilder(QueryBuilder):
    def _get_adapter(self) -> DatabaseAdapter:
        return MySqlAdapter()

class QuerySet:
    def __init__(self, model, alias: str = "default"):
        self.model = model
        self.alias = alias
        self.query_parts = {
            "select": ["*"],
            "where": [],
            "order_by": [],
            "limit": None,
            "offset": None,
            "joins": [],
            "group_by": [],
            "having": [],
            "with": [],
            "window": [],
        }
        self.params = []
        self._distinct = False
        self._distinct_on = []
        self._for_update = False
        self._for_share = False
        self._nowait = False
        self._skip_locked = False
        self._for_update_of = []
        self._no_key = False
        self._param_counter = 1
        self._selected_related = set()
        self._prefetch_related = set()
        self._related_joins = set() 
        self._builder = self._get_builder()

    def _get_db_type(self) -> DatabaseType:
        """Get database type from alias mapping."""
        return get_db_type_with_alias(self.alias)

    def _get_builder(self) -> QueryBuilder:
        db_type = self._get_db_type()
        match db_type:
            case DatabaseType.Postgres:
                return PostgresQueryBuilder(self)
            case DatabaseType.MySql:
                return MySqlQueryBuilder(self)
            case _:
                raise ValueError(f"Unsupported database type: {db_type}")

    def __get_next_param(self):
        placeholder = self._builder.adapter.get_placeholder(self._param_counter)
        self._param_counter += 1
        return placeholder

    def clone(self) -> "QuerySet":
        new_qs = QuerySet(self.model, self.alias)
        new_qs.query_parts = {
            k: v[:] if isinstance(v, list) else v for k, v in self.query_parts.items()
        }
        new_qs.params = self.params[:]
        new_qs._distinct = self._distinct
        new_qs._distinct_on = self._distinct_on[:]
        new_qs._for_update = self._for_update
        new_qs._for_share = self._for_share
        new_qs._nowait = self._nowait
        new_qs._skip_locked = self._skip_locked
        new_qs._for_update_of = self._for_update_of[:]
        new_qs._no_key = self._no_key
        new_qs._param_counter = self._param_counter
        new_qs._selected_related = self._selected_related.copy()
        new_qs._prefetch_related = self._prefetch_related.copy()
        return new_qs

    def select(self, *fields, distinct: bool = False) -> "QuerySet":
        qs = self.clone()
        qs.query_parts["select"] = list(
            map(lambda x: f"{self.model.table_name()}.{x}" if x != "*" else x, fields)
        )
        qs._distinct = distinct
        return qs

    def _process_q_object(self, q_obj: Q, params: List | None = None) -> Tuple[str, List]:
        if params is None:
            params = []

        if not q_obj.children:
            return "", params

        sql_parts = []
        local_params = []

        for child in q_obj.children:
            if isinstance(child, Q):
                inner_sql, inner_params = self._process_q_object(child)
                sql_parts.append(f"({inner_sql})")
                local_params.extend(inner_params)
            elif isinstance(child, dict):
                for key, value in child.items():
                    field_sql, field_params = self._process_where_item(key, value)
                    sql_parts.append(field_sql)
                    local_params.extend(field_params)
            elif isinstance(child, tuple):
                field_sql, field_params = self._process_where_item(child[0], child[1])
                sql_parts.append(field_sql)
                local_params.extend(field_params)

        joined = f" {q_obj.connector} ".join(sql_parts)
        if q_obj.negated:
            joined = f"NOT ({joined})"

        params.extend(local_params)
        return joined, params

    def _process_where_item(self, key: str, value: Any) -> Tuple[str, List]:
        parts = key.split("__")
        field = parts[0]
        op = "=" if len(parts) == 1 else parts[1]

        # Handle related model filters (e.g., company__name)
        if len(parts) > 1 and field in self.model._fields and isinstance(self.model._fields[field], ForeignKeyField):
            related_model = self.model._fields[field].to
            related_field = parts[1]
            op = parts[2] if len(parts) > 2 else "="
            join_key = f"{field}__{related_model.table_name()}"
            if join_key not in self._related_joins:
                self.query_parts["joins"].append(
                    f"LEFT JOIN {related_model.table_name()} ON {self.model.table_name()}.{field} = {related_model.table_name()}.{self.model._fields[field].related_field}"
                )
                self._related_joins.add(join_key)
            param_name = self.__get_next_param()
            formatted_op = self._builder.adapter.format_operator(op)
            return f"{related_model.table_name()}.{related_field} {formatted_op} {param_name}", [value]

        if isinstance(value, F):
            if value.params:
                param_name = self.__get_next_param()
                sql = value.field.replace("%s", param_name)
                return sql, value.params
            return self._process_f_value(field, op, value)
        if isinstance(value, Expression):
            return self._process_expression_value(field, op, value)
        return self._process_standard_value(field, op, value)

    def _process_f_value(self, field: str, op: str, value: F) -> Tuple[str, List]:
        formatted_op = self._builder.adapter.format_operator(op)
        return f"{self.model.table_name()}.{field} {formatted_op} {value.field}", []

    def _process_expression_value(self, field: str, op: str, value: Expression) -> Tuple[str, List]:
        formatted_op = self._builder.adapter.format_operator(op)
        return f"{self.model.table_name()}.{field} {formatted_op} {value.sql}", value.params

    def _process_standard_value(self, field: str, op: str, value: Any) -> Tuple[str, List]:
        op_map = {
            "gt": Operator.GT.value,
            "lt": Operator.LT.value,
            "gte": Operator.GTE.value,
            "lte": Operator.LTE.value,
            "contains": Operator.LIKE.value,
            "icontains": Operator.ILIKE.value,
            "startswith": Operator.LIKE.value,
            "endswith": Operator.LIKE.value,
            "in": Operator.IN.value,
            "not_in": Operator.NOT_IN.value,
            "isnull": Operator.IS_NULL.value,
            "between": Operator.BETWEEN.value,
            "regex": Operator.REGEXP.value,
            "iregex": Operator.IREGEXP.value,
        }

        formatted_op = self._builder.adapter.format_operator(op_map.get(op, op))
        if not self._builder.adapter.supports_operator(formatted_op):
            raise ValueError(f"Operator {op} not supported for alias {self.alias}")

        if op in op_map:
            return self._process_op_map_value(field, op, value, op_map)
        else:
            param_name = self.__get_next_param()
            return f"{self.model.table_name()}.{field} = {param_name}", [value]

    def _process_op_map_value(self, field: str, op: str, value: Any, op_map: dict) -> Tuple[str, List]:
        combine_field_name = f"{self.model.table_name()}.{field}"
        formatted_op = self._builder.adapter.format_operator(op_map[op])

        if op in ("contains", "icontains"):
            param_name = self.__get_next_param()
            return f"{combine_field_name} {formatted_op} {param_name}", [f"%{value}%"]
        elif op == "startswith":
            param_name = self.__get_next_param()
            return f"{combine_field_name} {formatted_op} {param_name}", [f"{value}%"]
        elif op == "endswith":
            param_name = self.__get_next_param()
            return f"{combine_field_name} {formatted_op} {param_name}", [f"%{value}"]
        elif op == "isnull":
            return (
                f"{combine_field_name} {Operator.IS_NULL.value if value else Operator.IS_NOT_NULL.value}",
                [],
            )
        elif op == "between":
            param1 = self.__get_next_param()
            param2 = self.__get_next_param()
            return f"{combine_field_name} {formatted_op} {param1} AND {param2}", [value[0], value[1]]
        elif op in ("in", "not_in"):
            placeholders = ",".join([self.__get_next_param() for _ in value])
            self._param_counter += len(value)
            return f"{combine_field_name} {formatted_op} ({placeholders})", list(value)
        else:
            param_name = self.__get_next_param()
            return f"{combine_field_name} {formatted_op} {param_name}", [value]

    def filter(self, *args, **kwargs) -> "QuerySet":
        """Filter records based on conditions, alias for where."""
        return self.where(*args, **kwargs)

    def exclude(self, *args, **kwargs) -> "QuerySet":
        """Exclude records matching the given conditions."""
        qs = self.clone()
        q = Q(*args, **kwargs)
        q = ~q  # Negate the Q object
        sql, params = qs._process_q_object(q)
        if sql:
            qs.query_parts["where"].append(sql)
            qs.params.extend(params)
        return qs
    
    def _convert_value(self, value: Any, type_str: str, field_type: Optional[str] = None) -> Any:
        """Convert a value to the appropriate Python type based on type string and optional field type."""
        if value is None:
            return None
        
        try:
            if type_str == "int" or field_type == "int":
                return int(value)
            elif type_str == "str" or field_type == "str":
                return str(value)
            elif type_str == "decimal" or field_type == "decimal":
                return decimal.Decimal(str(value))
            elif type_str == "datetime" or field_type == "datetime":
                if isinstance(value, str):
                    return datetime.datetime.fromisoformat(value)
                return value  # Assume datetime object
            elif type_str == "json" or field_type == "json":
                if isinstance(value, str):
                    return json.loads(value)
                return value  # Assume dict/list
            elif type_str == "array" or field_type == "array":
                if isinstance(value, list):
                    return value
                return list(value) if value else []
            else:
                return value  # Fallback to raw value
        except (ValueError, TypeError, json.JSONDecodeError) as e:
            print(f"Warning: Failed to convert value {value} of type {type_str}: {e}")
            return value
    
    def _parse_row(self, row: List[Tuple[str, Any, str]]) -> Any:
        parsed_data = {}
        related_data = {}
        row_dict = {col_name: (value, col_type) for col_name, value, col_type in row}

        # Process primary model fields
        for field_name, field in self.model._fields.items():
            # Try both table_name.field_name and field_name
            col_name = f"{self.model.table_name()}.{field_name}"
            alt_col_name = field_name
            if col_name in row_dict:
                value, col_type = row_dict[col_name]
                parsed_data[field_name] = self._convert_value(value, col_type, field.field_type)
            elif alt_col_name in row_dict:
                value, col_type = row_dict[alt_col_name]
                parsed_data[field_name] = self._convert_value(value, col_type, field.field_type)

        # Process foreign key fields
        for field_name in self._selected_related:
            if field_name in self.model._fields and isinstance(self.model._fields[field_name], ForeignKeyField):
                related_model = self.model._fields[field_name].to_model
                related_table = related_model.table_name()
                related_fields = {
                    k.split(f"{related_table}__")[1]: v
                    for k, v in row_dict.items()
                    if k.startswith(f"{related_table}__")
                }
                if related_fields and any(v[0] is not None for v in related_fields.values()):
                    related_data[field_name] = related_model(**{
                        k: self._convert_value(v[0], v[1], related_model._fields.get(k).field_type)
                        for k, v in related_fields.items()
                    })
                else:
                    related_data[field_name] = None

        instance = self.model(**parsed_data)
        instance._related_data = related_data
        return instance
    
    def _infer_aggregate_type(self, expr: Expression, field_name: str) -> str:
        """Infer the result type of an aggregation expression."""
        if "SUM(" in expr.sql or "AVG(" in expr.sql:
            if field_name in self.model._fields:
                return self.model._fields[field_name].field_type
            return "decimal"
        elif "COUNT(" in expr.sql:
            return "int"
        elif "MAX(" in expr.sql or "MIN(" in expr.sql:
            if field_name in self.model._fields:
                return self.model._fields[field_name].field_type
            return "str"
        return "str"
    
    def _parse_aggregate_row(self, row: List[Tuple[str, Any, str]], annotations: Dict[str, Expression]) -> Dict[str, Any]:
        """Parse raw aggregate row using type inference."""
        parsed = {}
        # Convert list of tuples to dict
        row_dict = {col_name: (value, col_type) for col_name, value, col_type in row}
        
        for alias, expr in annotations.items():
            if alias in row_dict:
                # Extract field name from expr.sql (e.g., SUM(res_partner.partner_latitude) -> partner_latitude)
                field_name = expr.sql.split("(")[-1].split(")")[0].split(".")[-1] if "(" in expr.sql else alias
                field_type = self._infer_aggregate_type(expr, field_name)
                value, col_type = row_dict[alias]
                parsed[alias] = self._convert_value(value, col_type, field_type)
        
        return parsed

    def get(self, *args, **kwargs) -> Any:
        """Fetch exactly one model instance matching the conditions."""
        qs = self.filter(*args, **kwargs)
        sql, params = qs.to_sql()
        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.fetch_all(sql, params)
        
        if not result:
            raise DoesNotExist(f"{self.model.__name__} matching query does not exist.")
        if len(result) > 1:
            raise MultipleObjectsReturned(
                f"get() returned {len(result)} objects, expected exactly one."
            )
        return self._parse_row(result[0])

    def first(self) -> Optional[Dict[str, Any]]:
        """Return the first record or None."""
        qs = self.clone()
        if not qs.query_parts["order_by"]:
            qs = qs.order_by("id")  # Default ordering
        qs = qs.limit(1)
        result = qs.execute()
        return result[0] if result else None

    def last(self) -> Optional[Dict[str, Any]]:
        """Return the last record or None."""
        qs = self.clone()
        if not qs.query_parts["order_by"]:
            qs = qs.order_by("-id")  # Default reverse ordering
        else:
            # Reverse existing order_by
            qs.query_parts["order_by"] = [
                f"-{field}" if not field.startswith("-") else field[1:]
                for field in qs.query_parts["order_by"]
            ]
        qs = qs.limit(1)
        result = qs.execute()
        return result[0] if result else None

    def none(self) -> "QuerySet":
        """Return an empty QuerySet."""
        qs = self.clone()
        qs.query_parts["where"] = ["1=0"]  # Always false condition
        qs.params = []
        return qs

    def all(self) -> "QuerySet":
        """Return all records (clone of current QuerySet)."""
        return self.clone()

    def distinct(self, *fields) -> "QuerySet":
        """Remove duplicate rows, optionally on specific fields (PostgreSQL only)."""
        qs = self.clone()
        if fields:
            if self._get_db_type() != "postgresql":
                raise ValueError("DISTINCT ON fields is only supported in PostgreSQL")
            qs._distinct_on = list(fields)
            qs._distinct = True
        else:
            qs._distinct = True
        return qs

    def select_for_update(
        self,
        nowait: bool = False,
        skip_locked: bool = False,
        of: Optional[List[str]] = None,
        no_key: bool = False
    ) -> "QuerySet":
        """Lock selected rows until transaction ends."""
        qs = self.clone()
        qs._for_update = True
        qs._nowait = nowait
        qs._skip_locked = skip_locked
        qs._for_update_of = of or []
        qs._no_key = no_key
        return qs

    def prefetch_related(self, *lookups) -> "QuerySet":
        qs = self.clone()
        for lookup in lookups:
            if lookup in qs.model._fields and isinstance(qs.model._fields[lookup], ForeignKeyField):
                qs._prefetch_related.add(lookup)
        return qs

    def aggregate(self, **annotations) -> Dict[str, Any]:
        """Compute aggregate values (e.g., SUM, COUNT) and return raw results."""
        if not annotations:
            return {}

        qs = self.clone()
        qs = qs.annotate(**annotations)
        qs.query_parts["select"] = [
            f"{expr.sql} AS {alias}" for alias, expr in annotations.items()
            if isinstance(expr, Expression)
        ]
        sql, params = qs.to_sql()
        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.fetch_all(sql, params)
        
        if not result:
            return {}
        
        return self._parse_aggregate_row(result[0], annotations)

    def where(self, *args, **kwargs) -> "QuerySet":
        qs = self.clone()
        for arg in args:
            if isinstance(arg, Q):
                sql, params = qs._process_q_object(arg, [])
                if sql:
                    qs.query_parts["where"].append(sql)
                    qs.params.extend(params)
            elif isinstance(arg, Expression):
                qs.query_parts["where"].append(arg.sql)
                qs.params.extend(arg.params)
            elif isinstance(arg, F):
                if arg.params:
                    param_name = self.__get_next_param()
                    sql = arg.field.replace("%s", param_name)
                    qs.query_parts["where"].append(sql)
                    qs.params.extend(arg.params)
                else:
                    qs.query_parts["where"].append(arg.field)
            else:
                qs.query_parts["where"].append(str(arg))

        if kwargs:
            q = Q(**kwargs)
            sql, params = qs._process_q_object(q, [])
            if sql:
                qs.query_parts["where"].append(sql)
                qs.params.extend(params)
        return qs

    def annotate(self, **annotations) -> "QuerySet":
        qs = self.clone()
        select_parts = []

        for alias, expression in annotations.items():
            if isinstance(expression, F):
                select_parts.append(f"{expression.field} AS {alias}")
            elif isinstance(expression, Expression):
                select_parts.append(f"({expression.sql}) AS {alias}")
                qs.params.extend(expression.params)
            else:
                select_parts.append(f"{expression} AS {alias}")

        qs.query_parts["select"].extend(select_parts)
        return qs

    def values(self, *fields) -> "QuerySet":
        return self.select(*fields)

    def values_list(self, *fields, flat: bool = False) -> "QuerySet":
        if flat and len(fields) > 1:
            raise ValueError(
                "'flat' is not valid when values_list is called with more than one field."
            )
        return self.select(*fields)

    def order_by(self, *fields) -> "QuerySet":
        qs = self.clone()
        order_parts = []

        for field in fields:
            if isinstance(field, F):
                order_parts.append(field.field)
            elif isinstance(field, Expression):
                order_parts.append(field.sql)
                qs.params.extend(field.params)
            elif field.startswith("-"):
                order_parts.append(f"{field[1:]} DESC")
            else:
                order_parts.append(f"{self.model.table_name()}.{field} ASC")

        qs.query_parts["order_by"] = order_parts
        return qs

    def select_related(self, *fields) -> "QuerySet":
        qs = self.clone()
        for field in fields:
            if field in qs.model._fields and isinstance(qs.model._fields[field], ForeignKeyField):
                qs._selected_related.add(field)
                related_model = qs.model._fields[field].to_model
                join_key = f"{field}__{related_model.table_name()}"
                # Check if a join for this table already exists
                join_exists = any(
                    related_model.table_name() in join
                    for join in qs.query_parts["joins"]
                )
                if not join_exists:
                    qs.query_parts["joins"].append(
                        f"LEFT JOIN {related_model.table_name()} ON {qs.model.table_name()}.{field} = {related_model.table_name()}.{qs.model._fields[field].related_field}"
                    )
                    qs._related_joins.add(join_key)
                # Add related fields with aliases
                qs.query_parts["select"].extend([
                    f"{related_model.table_name()}.{related_field} AS {related_model.table_name()}__{related_field}"
                    for related_field in related_model._fields
                ])
        # Replace '*' with specific fields for the primary model
        if "*" in qs.query_parts["select"]:
            qs.query_parts["select"] = [
                f"{qs.model.table_name()}.{field}"
                for field in qs.model._fields
            ] + [s for s in qs.query_parts["select"] if s != "*"]
        return qs

    def join(self, table: Any, on: Union[str, Expression, bool], join_type: Union[str, JoinType] = JoinType.INNER) -> "QuerySet":
        qs = self.clone()
        joined_table = table.table_name() if hasattr(table, "table_name") else table
        if isinstance(join_type, JoinType):
            join_type = join_type.value
        if isinstance(on, Expression):
            qs.query_parts["joins"].append(f"{join_type} {joined_table} ON {on.sql}")
            qs.params.extend(on.params)
        elif isinstance(on, F):
            qs.query_parts["joins"].append(f"{join_type} {joined_table} ON {on.field}")
        else:
            qs.query_parts["joins"].append(f"{join_type} {joined_table} ON {on}")
        return qs

    def group_by(self, *fields) -> "QuerySet":
        qs = self.clone()
        group_parts = []

        for field in fields:
            if isinstance(field, F):
                group_parts.append(field.field)
            elif isinstance(field, Expression):
                group_parts.append(field.sql)
                qs.params.extend(field.params)
            else:
                group_parts.append(f"{self.model.table_name()}.{field}")

        qs.query_parts["group_by"] = group_parts
        return qs

    def having(self, *conditions) -> "QuerySet":
        qs = self.clone()
        having_parts = []

        for condition in conditions:
            if isinstance(condition, Expression):
                having_parts.append(condition.sql)
                qs.params.extend(condition.params)
            else:
                having_parts.append(str(condition))

        qs.query_parts["having"] = having_parts
        return qs

    def window(self, alias: str, partition_by: List = None, order_by: List = None) -> "QuerySet":
        qs = self.clone()
        parts = [f"{alias} AS ("]

        if partition_by:
            parts.append(qs._process_partition_by(partition_by, qs))

        if order_by:
            parts.append(qs._process_order_by(order_by, qs))

        parts.append(")")
        qs.query_parts["window"].append(" ".join(parts))
        return qs

    def _process_partition_by(self, partition_by: List, qs: "QuerySet") -> str:
        partition_parts = []
        for field in partition_by:
            if isinstance(field, F):
                partition_parts.append(field.field)
            elif isinstance(field, Expression):
                partition_parts.append(field.sql)
                qs.params.extend(field.params)
            else:
                partition_parts.append(f"{self.model.table_name()}.{field}")
        return f"PARTITION BY {', '.join(partition_parts)}"

    def _process_order_by(self, order_by: List, qs: "QuerySet") -> str:
        order_parts = []
        for field in order_by:
            if isinstance(field, F):
                order_parts.append(field.field)
            elif isinstance(field, Expression):
                order_parts.append(field.sql)
                qs.params.extend(field.params)
            elif field.startswith("-"):
                order_parts.append(f"{self.model.table_name()}.{field[1:]} DESC")
            else:
                order_parts.append(f"{self.model.table_name()}.{field} ASC")
        return f"ORDER BY {', '.join(order_parts)}"

    def limit(self, limit: int) -> "QuerySet":
        qs = self.clone()
        qs.query_parts["limit"] = limit
        return qs

    def offset(self, offset: int) -> "QuerySet":
        qs = self.clone()
        qs.query_parts["offset"] = offset
        return qs

    def for_update(self, nowait: bool = False, skip_locked: bool = False) -> "QuerySet":
        qs = self.clone()
        qs._for_update = True
        qs._nowait = nowait
        qs._skip_locked = skip_locked
        return qs

    def for_share(self, nowait: bool = False, skip_locked: bool = False) -> "QuerySet":
        qs = self.clone()
        qs._for_share = True
        qs._nowait = nowait
        qs._skip_locked = skip_locked
        return qs

    def with_recursive(self, name: str, initial_query: str, recursive_query: str) -> "QuerySet":
        qs = self.clone()
        cte = f"WITH RECURSIVE {name} AS ({initial_query} UNION ALL {recursive_query})"
        qs.query_parts["with"].append(cte)
        return qs

    def union(self, other_qs: "QuerySet", all: bool = False) -> "QuerySet":
        sql1, params1 = self.to_sql()
        sql2, params2 = other_qs.to_sql()
        union_type = "UNION ALL" if all else "UNION"
        combined_sql = f"({sql1}) {union_type} ({sql2})"
        combined_params = params1 + params2

        new_qs = self.clone()
        new_qs.query_parts["raw_sql"] = combined_sql
        new_qs.params = combined_params
        return new_qs

    def intersect(self, other_qs: "QuerySet", all: bool = False) -> "QuerySet":
        sql1, params1 = self.to_sql()
        sql2, params2 = other_qs.to_sql()
        intersect_type = "INTERSECT ALL" if all else "INTERSECT"
        combined_sql = f"({sql1}) {intersect_type} ({sql2})"
        combined_params = params1 + params2

        new_qs = self.clone()
        new_qs.query_parts["raw_sql"] = combined_sql
        new_qs.params = combined_params
        return new_qs

    def except_(self, other_qs: "QuerySet", all: bool = False) -> "QuerySet":
        sql1, params1 = self.to_sql()
        sql2, params2 = other_qs.to_sql()
        except_type = "EXCEPT ALL" if all else "EXCEPT"
        combined_sql = f"({sql1}) {except_type} ({sql2})"
        combined_params = params1 + params2

        new_qs = self.clone()
        new_qs.query_parts["raw_sql"] = combined_sql
        new_qs.params = combined_params
        return new_qs

    def subquery(self, alias: str) -> Expression:
        sql, params = self.to_sql()
        return Expression(f"({sql}) AS {alias}", params)

    def to_sql(self) -> Tuple[str, List]:
        if "raw_sql" in self.query_parts:
            return self.query_parts["raw_sql"], self.params
        return self._builder.build_sql()

    def _add_with_clause(self, parts):
        if self.query_parts["with"]:
            parts.append(" ".join(self.query_parts["with"]))

    def _add_select_clause(self, parts):
        select_clause = "SELECT"
        if self._distinct:
            if self._distinct_on:
                fields = ", ".join(self._distinct_on)
                select_clause += f" DISTINCT ON ({fields})"
            else:
                select_clause += " DISTINCT"
        select_clause += " " + ", ".join(self.query_parts["select"])
        parts.append(select_clause)

    def _add_from_clause(self, parts):
        parts.append(f"FROM {self.model.table_name()}")

    def _add_joins_clause(self, parts):
        if self.query_parts["joins"]:
            parts.extend(self.query_parts["joins"])

    def _add_where_clause(self, parts):
        if self.query_parts["where"]:
            parts.append(
                "WHERE "
                + " AND ".join(
                    f"({condition})" for condition in self.query_parts["where"]
                )
            )

    def _add_group_by_clause(self, parts):
        if self.query_parts["group_by"]:
            parts.append("GROUP BY " + ", ".join(self.query_parts["group_by"]))

    def _add_having_clause(self, parts):
        if self.query_parts["having"]:
            parts.append("HAVING " + " AND ".join(self.query_parts["having"]))

    def _add_window_clause(self, parts):
        if self.query_parts["window"]:
            parts.append("WINDOW " + ", ".join(self.query_parts["window"]))

    def _add_order_by_clause(self, parts):
        if self.query_parts["order_by"]:
            parts.append("ORDER BY " + ", ".join(self.query_parts["order_by"]))

    def _add_limit_clause(self, parts):
        if self.query_parts["limit"] is not None:
            parts.append(f"LIMIT {self.query_parts['limit']}")

    def _add_offset_clause(self, parts):
        if self.query_parts["offset"] is not None:
            parts.append(f"OFFSET {self.query_parts['offset']}")

    def _add_locking_clauses(self, parts):
        if self._for_update:
            lock_clause = "FOR UPDATE"
            if self._for_update_of:
                lock_clause += f" OF {', '.join(self._for_update_of)}"
            if self._no_key:
                lock_clause += " NO KEY"
            if self._nowait:
                lock_clause += " NOWAIT"
            elif self._skip_locked:
                lock_clause += " SKIP LOCKED"
            parts.append(lock_clause)
        elif self._for_share:
            lock_clause = "FOR SHARE"
            if self._for_update_of:
                lock_clause += f" OF {', '.join(self._for_update_of)}"
            if self._no_key:
                lock_clause += " NO KEY"
            if self._nowait:
                lock_clause += " NOWAIT"
            elif self._skip_locked:
                lock_clause += " SKIP LOCKED"
            parts.append(lock_clause)

    def execute(self) -> List[Dict[str, Any]]:
        """Execute the query and return results, handling prefetch_related."""
        sql, params = self.to_sql()
        session = self.model.get_session(alias=self.alias)
        print(f"Executing SQL: {sql} with params: {params}")
        with session as tx:
            result = tx.fetch_all(sql, params)
        # Parse rows into model instances
        instances = [self._parse_row(row) for row in result]
        if self._prefetch_related:
            instances = self._handle_prefetch_related(instances, result)
        
        return instances

    def _handle_prefetch_related(self, results: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Fetch related objects for prefetch_related lookups."""
        for lookup in self._prefetch_related:
            field = self.model._fields[lookup]
            related_model = field.to_model
            related_field = field.related_field

            # Get unique foreign key values
            fk_values = {row[lookup] for row in results if row[lookup] is not None}
            if not fk_values:
                continue

            # Query related objects
            related_qs = related_model.objects(alias=self.alias).filter(
                **{f"{related_field}__in": fk_values}
            )
            related_data = related_qs.execute()

            # Map foreign key values to related objects
            fk_to_related = {row[related_field]: row for row in related_data}

            # Attach related objects to results
            for row in results:
                fk_value = row.get(lookup)
                if fk_value in fk_to_related:
                    row[f"{lookup}_data"] = fk_to_related[fk_value]
                else:
                    row[f"{lookup}_data"] = None

        return results

    def count(self) -> int:
        """Return the count of rows that would be returned by this query."""
        qs = self.clone()
        qs.query_parts["select"] = ["COUNT(*)"]
        qs.query_parts["order_by"] = []
        sql, params = qs.to_sql()
        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.fetch_all(sql, params)
        return result[0]["count"] if result else 0

    def exists(self) -> bool:
        """Return True if the query would return any results."""
        qs = self.clone()
        qs.query_parts["select"] = ["1"]
        qs.query_parts["order_by"] = []
        qs = qs.limit(1)
        sql, params = qs.to_sql()
        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.fetch_all(sql, params)
        return bool(result)

    def update(self, **kwargs) -> int:
        """Update records that match the query conditions."""
        updates = []
        params = []

        for field, value in kwargs.items():
            param_name = self.__get_next_param()
            if isinstance(value, F):
                updates.append(f"{field} = {value.field}")
            elif isinstance(value, Expression):
                updates.append(f"{field} = {value.sql}")
                params.extend(value.params)
            else:
                updates.append(f"{field} = {param_name}")
                params.append(value)

        where_sql = " AND ".join(
            f"({condition})" for condition in self.query_parts["where"]
        )

        sql = f"UPDATE {self.model.table_name()} SET {', '.join(updates)}"
        if where_sql:
            sql += f" WHERE {where_sql}"
        params = self.params + params
        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.bulk_change(sql, [params], 1)
        return result or 0

    def delete(self) -> int:
        """Delete records that match the query conditions."""
        where_sql = " AND ".join(
            f"({condition})" for condition in self.query_parts["where"]
        )

        sql = f"DELETE FROM {self.model.table_name()}"
        if where_sql:
            sql += f" WHERE {where_sql}"
        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.bulk_change(sql, [self.params], 1)
        return result or 0

    def bulk_create(self, objs: List[Any], batch_size: int | None = None) -> Optional[int]:
        """Insert multiple records in an efficient way."""
        if not objs:
            return None

        fields = [
            name for name, f in self.model._fields.items() if not f.auto_increment
        ]
        placeholders = ",".join([self.__get_next_param() for _ in fields])
        self._param_counter += len(fields) * len(objs)

        sql = f"INSERT INTO {self.model.table_name()} ({','.join(fields)}) VALUES ({placeholders})"

        values = [[obj._data.get(name, None) for name in fields] for obj in objs]

        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.bulk_change(sql, values, batch_size or len(values))
        return result

    def explain(self, analyze: bool = False, verbose: bool = False, costs: bool = False, buffers: bool = False, timing: bool = False) -> Dict:
        """Get the query execution plan."""
        options = []
        if analyze:
            options.append("ANALYZE")
        if verbose:
            options.append("VERBOSE")
        if costs:
            options.append("COSTS")
        if buffers:
            options.append("BUFFERS")
        if timing:
            options.append("TIMING")

        sql, params = self.to_sql()
        explain_sql = f"EXPLAIN ({' '.join(options)}) {sql}"
        session = self.model.get_session(alias=self.alias)
        with session as tx:
            result = tx.fetch_all(explain_sql, params)
        return result