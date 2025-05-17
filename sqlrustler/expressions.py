from typing import Any, List, Tuple

from .enum import Operator
from .express import Expression
from .F import F
from .field import ForeignKeyField
from .Q import Q


class ExpressionHandler:
    def __init__(self, queryset):
        self.queryset = queryset

    def process_q_object(
        self, q_obj: Q, params: List[Any] = None
    ) -> Tuple[str, List[Any]]:
        params = params or []
        if not q_obj.children:
            return "", params

        sql_parts = []
        local_params = []
        for child in q_obj.children:
            if isinstance(child, Q):
                inner_sql, inner_params = self.process_q_object(child)
                sql_parts.append(f"({inner_sql})")
                local_params.extend(inner_params)
            elif isinstance(child, dict):
                for key, value in child.items():
                    field_sql, field_params = self.process_where_item(key, value)
                    sql_parts.append(field_sql)
                    local_params.extend(field_params)
            elif isinstance(child, tuple):
                field_sql, field_params = self.process_where_item(child[0], child[1])
                sql_parts.append(field_sql)
                local_params.extend(field_params)

        joined = f" {q_obj.connector} ".join(sql_parts)
        if q_obj.negated:
            joined = f"NOT ({joined})"
        params.extend(local_params)
        return joined, params

    def process_f_object(self, f_obj: F) -> Tuple[str, List[Any]]:
        if f_obj.params:
            param_name = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter
            )
            self.queryset._param_counter += 1
            sql = f_obj.field.replace("%s", param_name)
            return sql, f_obj.params
        return f_obj.field, []

    def process_where_item(self, key: str, value: Any) -> Tuple[str, List[Any]]:
        parts = key.split("__")
        field = parts[0]
        op = "=" if len(parts) == 1 else parts[1]

        if (
            len(parts) > 1
            and field in self.queryset.model._fields
            and isinstance(self.queryset.model._fields[field], ForeignKeyField)
        ):
            related_model = self.queryset.model._fields[field].to_model
            related_field = parts[1]
            op = parts[2] if len(parts) > 2 else "="
            join_key = f"{field}__{related_model.table_name()}"
            if join_key not in self.queryset._related_joins:
                self.queryset.state["joins"].append(
                    f"LEFT JOIN {related_model.table_name()} ON {self.queryset.model.table_name()}.{field} = {related_model.table_name()}.{self.queryset.model._fields[field].related_field}"
                )
                self.queryset._related_joins.add(join_key)
            param_name = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter
            )
            self.queryset._param_counter += 1
            formatted_op = self.queryset._adapter.format_operator(op)
            return (
                f"{related_model.table_name()}.{related_field} {formatted_op} {param_name}",
                [value],
            )

        if isinstance(value, F):
            return self.process_f_value(field, op, value)
        elif isinstance(value, Expression):
            return self.process_expression_value(field, op, value)
        return self.process_standard_value(field, op, value)

    def process_f_value(self, field: str, op: str, value: F) -> Tuple[str, List[Any]]:
        formatted_op = self.queryset._adapter.format_operator(op)
        return (
            f"{self.queryset.model.table_name()}.{field} {formatted_op} {value.field}",
            [],
        )

    def process_expression_value(
        self, field: str, op: str, value: Expression
    ) -> Tuple[str, List[Any]]:
        formatted_op = self.queryset._adapter.format_operator(op)
        return (
            f"{self.queryset.model.table_name()}.{field} {formatted_op} {value.sql}",
            value.params,
        )

    def process_standard_value(
        self, field: str, op: str, value: Any
    ) -> Tuple[str, List[Any]]:
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
        formatted_op = self.queryset._adapter.format_operator(op_map.get(op, op))
        if not self.queryset._adapter.supports_operator(formatted_op):
            raise ValueError(
                f"Operator {op} not supported for alias {self.queryset.alias}"
            )

        combine_field_name = f"{self.queryset.model.table_name()}.{field}"
        if op == "contains" or op == "icontains":
            param_name = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter
            )
            self.queryset._param_counter += 1
            return f"{combine_field_name} {formatted_op} {param_name}", [f"%{value}%"]
        elif op == "startswith":
            param_name = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter
            )
            self.queryset._param_counter += 1
            return f"{combine_field_name} {formatted_op} {param_name}", [f"{value}%"]
        elif op == "endswith":
            param_name = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter
            )
            self.queryset._param_counter += 1
            return f"{combine_field_name} {formatted_op} {param_name}", [f"%{value}"]
        elif op == "isnull":
            return f"{combine_field_name} {'IS NULL' if value else 'IS NOT NULL'}", []
        elif op == "between":
            param1 = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter
            )
            param2 = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter + 1
            )
            self.queryset._param_counter += 2
            return f"{combine_field_name} {formatted_op} {param1} AND {param2}", [
                value[0],
                value[1],
            ]
        elif op in ("in", "not_in"):
            placeholders = ",".join(
                [
                    self.queryset._adapter.get_placeholder(
                        self.queryset._param_counter + i
                    )
                    for i in range(len(value))
                ]
            )
            self.queryset._param_counter += len(value)
            return f"{combine_field_name} {formatted_op} ({placeholders})", list(value)
        else:
            param_name = self.queryset._adapter.get_placeholder(
                self.queryset._param_counter
            )
            self.queryset._param_counter += 1
            return f"{combine_field_name} {formatted_op} {param_name}", [value]
