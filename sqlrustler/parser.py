import datetime
import decimal
import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from .express import Expression
from .field import ForeignKeyField

# Configure logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)


class ResultParser:
    def __init__(self, queryset):
        self.queryset = queryset

    def parse_row(self, row: List[Tuple[str, Any, str]], raw: bool = False) -> Any:
        """Parse a database row into a Model instance or raw dict."""
        row_dict = {col_name: (value, col_type) for col_name, value, col_type in row}

        if raw or self.queryset._raw_results:
            # Return raw dictionary with converted values
            return {
                col_name: self._convert_value(value, col_type, "str")
                for col_name, (value, col_type) in row_dict.items()
            }

        parsed_data = {}
        annotations = {}
        related_data = {}

        # Process primary model fields
        for field_name, field in self.queryset.model._fields.items():
            col_name = f"{self.queryset.model.table_name()}.{field_name}"
            alt_col_name = field_name
            if col_name in row_dict:
                value, col_type = row_dict[col_name]
                parsed_data[field_name] = self._convert_value(
                    value, col_type, field.field_type
                )
            elif alt_col_name in row_dict:
                value, col_type = row_dict[alt_col_name]
                parsed_data[field_name] = self._convert_value(
                    value, col_type, field.field_type
                )
            else:
                logger.warning(
                    f"Field {field_name} not found in query result for {self.queryset.model.__name__}"
                )

        # Process annotated fields
        for col_name in self.queryset.state["annotations"]:
            if col_name in row_dict:
                value, col_type = row_dict[col_name]
                annotations[col_name] = self._convert_value(
                    value, col_type, "int" if col_name == "row_num" else "str"
                )
            else:
                logger.warning(f"Annotation {col_name} not found in query result")

        # Process foreign key fields
        for field_name in self.queryset._selected_related:
            if field_name in self.queryset.model._fields and isinstance(
                self.queryset.model._fields[field_name], ForeignKeyField
            ):
                related_model = self.queryset.model._fields[field_name].to_model
                related_table = related_model.table_name()
                related_fields = {
                    k.split(f"{related_table}__")[1]: v
                    for k, v in row_dict.items()
                    if k.startswith(f"{related_table}__")
                }
                if related_fields and any(
                    v[0] is not None for v in related_fields.values()
                ):
                    try:
                        related_data[field_name] = related_model(
                            **{
                                k: self._convert_value(
                                    v[0], v[1], related_model._fields.get(k).field_type
                                )
                                for k, v in related_fields.items()
                            }
                        )
                    except ValueError as e:
                        logger.warning(
                            f"Failed to parse related model {related_model.__name__} for {field_name}: {e}"
                        )
                        related_data[field_name] = None
                else:
                    related_data[field_name] = None

        # Log unknown columns
        expected_cols = (
            {
                f"{self.queryset.model.table_name()}.{f}"
                for f in self.queryset.model._fields
            }
            | {f for f in self.queryset.model._fields}
            | self.queryset.state["annotations"]
            | {
                f"{self.queryset.model._fields[field_name].to_model.table_name()}__{f}"
                for field_name in self.queryset._selected_related
                if isinstance(
                    self.queryset.model._fields.get(field_name), ForeignKeyField
                )
                for f in self.queryset.model._fields[field_name].to_model._fields
            }
        )
        for col_name in row_dict:
            if col_name not in expected_cols:
                logger.warning(
                    f"Unexpected column {col_name} in query result for {self.queryset.model.__name__}"
                )

        try:
            instance = self.queryset.model(**parsed_data)
            instance._related_data = related_data
            instance._annotations = annotations  # Store annotations separately
            return instance
        except ValueError as e:
            logger.error(
                f"Failed to create {self.queryset.model.__name__} instance: {e}"
            )
            # Fallback to raw dict
            return {
                **{k: v for k, v in parsed_data.items()},
                **annotations,
                **{f"related_{k}": v for k, v in related_data.items()},
            }

    def parse_aggregate_row(
        self, row: List[Tuple[str, Any, str]], annotations: Dict[str, Expression]
    ) -> Dict[str, Any]:
        """Parse aggregate query results."""
        parsed = {}
        row_dict = {col_name: (value, col_type) for col_name, value, col_type in row}
        for alias, expr in annotations.items():
            if alias in row_dict:
                field_name = (
                    expr.sql.split("(")[-1].split(")")[0].split(".")[-1]
                    if "(" in expr.sql
                    else alias
                )
                field_type = self._infer_aggregate_type(expr, field_name)
                value, col_type = row_dict[alias]
                parsed[alias] = self._convert_value(value, col_type, field_type)
            else:
                logger.warning(f"Aggregate {alias} not found in query result")
        return parsed

    def handle_prefetch_related(self, instances: List[Any], lookups: set) -> List[Any]:
        """Fetch and attach related objects for prefetch_related."""
        for lookup in lookups:
            if lookup in self.queryset.model._fields and isinstance(
                self.queryset.model._fields[lookup], ForeignKeyField
            ):
                field = self.queryset.model._fields[lookup]
                related_model = field.to_model
                related_field = field.related_field
                fk_values = {
                    getattr(instance, lookup, None)
                    for instance in instances
                    if getattr(instance, lookup, None) is not None
                }
                if not fk_values:
                    continue
                related_qs = related_model.objects(alias=self.queryset.alias).filter(
                    **{f"{related_field}__in": fk_values}
                )
                related_data = related_qs.execute()
                fk_to_related = {
                    getattr(row, related_field): row for row in related_data
                }
                for instance in instances:
                    fk_value = getattr(instance, lookup, None)
                    instance._related_data[lookup] = fk_to_related.get(fk_value)
        return instances

    def _convert_value(
        self, value: Any, type_str: str, field_type: Optional[str] = None
    ) -> Any:
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
                return value
            elif type_str == "json" or field_type == "json":
                if isinstance(value, str):
                    return json.loads(value)
                return value
            elif type_str == "array" or field_type == "array":
                if isinstance(value, list):
                    return value
                return list(value) if value else []
            else:
                return value
        except (ValueError, TypeError, json.JSONDecodeError) as e:
            logger.warning(f"Failed to convert value {value} of type {type_str}: {e}")
            return value

    def _infer_aggregate_type(self, expr: Expression, field_name: str) -> str:
        if "SUM(" in expr.sql or "AVG(" in expr.sql:
            return self.queryset.model._fields.get(
                field_name, type("Field", (), {"field_type": "decimal"})()
            ).field_type
        elif "COUNT(" in expr.sql:
            return "int"
        elif "MAX(" in expr.sql or "MIN(" in expr.sql:
            return self.queryset.model._fields.get(
                field_name, type("Field", (), {"field_type": "str"})()
            ).field_type
        return "str"
