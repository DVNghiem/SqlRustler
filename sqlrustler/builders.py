from typing import Any, Dict, List, Optional

from .express import Expression
from .F import F


class QueryBuilder:
    def __init__(self, queryset):
        self.queryset = queryset
        self.adapter = queryset._adapter

    def get_placeholder(self, counter: int) -> str:
        return self.adapter.get_placeholder(counter)

    def build_sql(self) -> tuple[str, List[Any]]:
        raise NotImplementedError


class SelectBuilder(QueryBuilder):
    def build_sql(self) -> tuple[str, List[Any]]:
        parts = []
        self._add_with_clause(parts)
        self._add_select_clause(parts)
        self._add_from_clause(parts)
        self._add_joins_clause(parts)
        self._add_where_clause(parts)
        self._add_group_by_clause(parts)
        self._add_having_clause(parts)
        self._add_window_clause(parts)
        self._add_order_by_clause(parts)
        self._add_limit_clause(parts)
        self._add_offset_clause(parts)
        self._add_locking_clauses(parts)
        return " ".join(parts), self.queryset.params

    def _add_with_clause(self, parts: List[str]):
        if self.queryset.state["with"]:
            parts.append(" ".join(self.queryset.state["with"]))

    def _add_select_clause(self, parts: List[str]):
        select_clause = "SELECT"
        if self.queryset.state["distinct"]:
            if self.queryset.state["distinct_on"]:
                fields = ", ".join(self.queryset.state["distinct_on"])
                select_clause += f" DISTINCT ON ({fields})"
            else:
                select_clause += " DISTINCT"
        select_clause += " " + ", ".join(self.queryset.state["select"])
        parts.append(select_clause)

    def _add_from_clause(self, parts: List[str]):
        parts.append(f"FROM {self.queryset.model.table_name()}")

    def _add_joins_clause(self, parts: List[str]):
        parts.extend(self.queryset.state["joins"])

    def _add_where_clause(self, parts: List[str]):
        if self.queryset.state["where"]:
            parts.append(
                "WHERE " + " AND ".join(f"({c})" for c in self.queryset.state["where"])
            )

    def _add_group_by_clause(self, parts: List[str]):
        if self.queryset.state["group_by"]:
            parts.append("GROUP BY " + ", ".join(self.queryset.state["group_by"]))

    def _add_having_clause(self, parts: List[str]):
        if self.queryset.state["having"]:
            parts.append("HAVING " + " AND ".join(self.queryset.state["having"]))

    def _add_window_clause(self, parts: List[str]):
        if self.queryset.state["window"]:
            parts.append("WINDOW " + ", ".join(self.queryset.state["window"]))

    def _add_order_by_clause(self, parts: List[str]):
        if self.queryset.state["order_by"]:
            parts.append("ORDER BY " + ", ".join(self.queryset.state["order_by"]))

    def _add_limit_clause(self, parts: List[str]):
        if self.queryset.state["limit"] is not None:
            parts.append(f"LIMIT {self.queryset.state['limit']}")

    def _add_offset_clause(self, parts: List[str]):
        if self.queryset.state["offset"] is not None:
            parts.append(f"OFFSET {self.queryset.state['offset']}")

    def _add_locking_clauses(self, parts: List[str]):
        if self.queryset.state["for_update"]:
            lock_clause = "FOR UPDATE"
            if self.queryset.state["for_update_of"]:
                lock_clause += f" OF {', '.join(self.queryset.state['for_update_of'])}"
            if self.queryset.state["no_key"]:
                lock_clause += " NO KEY"
            if self.queryset.state["nowait"]:
                lock_clause += " NOWAIT"
            elif self.queryset.state["skip_locked"]:
                lock_clause += " SKIP LOCKED"
            parts.append(lock_clause)
        elif self.queryset.state["for_share"]:
            lock_clause = "FOR SHARE"
            if self.queryset.state["for_update_of"]:
                lock_clause += f" OF {', '.join(self.queryset.state['for_update_of'])}"
            if self.queryset.state["no_key"]:
                lock_clause += " NO KEY"
            if self.queryset.state["nowait"]:
                lock_clause += " NOWAIT"
            elif self.queryset.state["skip_locked"]:
                lock_clause += " SKIP LOCKED"
            parts.append(lock_clause)


class InsertBuilder(QueryBuilder):
    def execute_bulk_create(
        self, objs: List[Any], batch_size: Optional[int]
    ) -> Optional[int]:
        if not objs:
            return None
        fields = [
            name
            for name, f in self.queryset.model._fields.items()
            if not f.auto_increment
        ]
        placeholders = ",".join(
            [
                self.get_placeholder(self.queryset._param_counter + i)
                for i in range(len(fields))
            ]
        )
        self.queryset._param_counter += len(fields) * len(objs)
        sql = f"INSERT INTO {self.queryset.model.table_name()} ({','.join(fields)}) VALUES ({placeholders})"
        values = [[obj._data.get(name, None) for name in fields] for obj in objs]
        with self.queryset.model.get_session(alias=self.queryset.alias) as tx:
            return tx.bulk_change(sql, values, batch_size or len(values))


class UpdateBuilder(QueryBuilder):
    def execute_update(self, kwargs: Dict[str, Any]) -> int:
        updates = []
        params = []
        for field, value in kwargs.items():
            param_name = self.get_placeholder(self.queryset._param_counter)
            self.queryset._param_counter += 1
            if isinstance(value, F):
                updates.append(f"{field} = {value.field}")
            elif isinstance(value, Expression):
                updates.append(f"{field} = {value.sql}")
                params.extend(value.params)
            else:
                updates.append(f"{field} = {param_name}")
                params.append(value)
        sql = f"UPDATE {self.queryset.model.table_name()} SET {', '.join(updates)}"
        if self.queryset.state["where"]:
            sql += (
                f" WHERE {' AND '.join(f'({c})' for c in self.queryset.state['where'])}"
            )
        params = self.queryset.params + params
        with self.queryset.model.get_session(alias=self.queryset.alias) as tx:
            return tx.bulk_change(sql, [params], 1) or 0


class DeleteBuilder(QueryBuilder):
    def execute_delete(self) -> int:
        sql = f"DELETE FROM {self.queryset.model.table_name()}"
        if self.queryset.state["where"]:
            sql += (
                f" WHERE {' AND '.join(f'({c})' for c in self.queryset.state['where'])}"
            )
        with self.queryset.model.get_session(alias=self.queryset.alias) as tx:
            return tx.bulk_change(sql, [self.queryset.params], 1) or 0
