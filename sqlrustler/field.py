import json
from abc import ABC, abstractmethod
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Optional, Union

from sqlrustler.exceptions import DBFieldValidationError, DoesNotExist

from .F import F


class TypeMapper(ABC):
    """Strategy for mapping Python field types to database-specific SQL types."""

    @abstractmethod
    def get_sql_type(self, field_type: str, **kwargs) -> str:
        pass


class PostgresTypeMapper(TypeMapper):
    def get_sql_type(self, field_type: str, **kwargs) -> str:
        type_mapping = {
            "int": "INTEGER",
            "str": f"VARCHAR({kwargs.get('max_length', 255)})",
            "float": "FLOAT",
            "bool": "BOOLEAN",
            "datetime": "TIMESTAMP",
            "date": "DATE",
            "text": "TEXT",
            "json": "JSONB",
            "array": f"{kwargs.get('base_field').sql_type()}[]",
            "decimal": f"DECIMAL({kwargs.get('max_digits', 10)},{kwargs.get('decimal_places', 2)})",
        }
        return type_mapping.get(field_type, "VARCHAR(255)")


class MySqlTypeMapper(TypeMapper):
    def get_sql_type(self, field_type: str, **kwargs) -> str:
        type_mapping = {
            "int": "INTEGER",
            "str": f"VARCHAR({kwargs.get('max_length', 255)})",
            "float": "FLOAT",
            "bool": "TINYINT(1)",
            "datetime": "DATETIME",
            "date": "DATE",
            "text": "TEXT",
            "json": "JSON",
            "array": "TEXT",  # MySQL doesn't support arrays; store as JSON or TEXT
            "decimal": f"DECIMAL({kwargs.get('max_digits', 10)},{kwargs.get('decimal_places', 2)})",
        }
        return type_mapping.get(field_type, "VARCHAR(255)")


class FieldFactory:
    """Factory for creating database-specific Field instances."""

    @staticmethod
    def create_field(database_type: str, field_type: str, **kwargs) -> "Field":
        field_classes = {
            "postgres": {
                "int": IntegerField,
                "str": CharField,
                "float": FloatField,
                "bool": BooleanField,
                "datetime": DateTimeField,
                "date": DateField,
                "text": TextField,
                "json": JSONField,
                "array": ArrayField,
                "decimal": DecimalField,
            },
            "mysql": {
                "int": IntegerField,
                "str": CharField,
                "float": FloatField,
                "bool": BooleanField,
                "datetime": DateTimeField,
                "date": DateField,
                "text": TextField,
                "json": JSONField,
                "array": ArrayField,
                "decimal": DecimalField,
            },
        }
        cls = field_classes.get(database_type.lower(), {}).get(field_type, Field)
        return cls(field_type=field_type, **kwargs)


class Field:
    """Base field class for ORM-like field definitions."""

    def __init__(
        self,
        field_type: str,
        primary_key: bool = False,
        null: bool = True,
        default: Any = None,
        unique: bool = False,
        index: bool = False,
        validators: Optional[list] = None,
        auto_increment: bool = False,
        base_field: Optional["Field"] = None,
        to_model: Optional[Any] = None,  # Related model for foreign keys
    ):
        self.field_type = field_type
        self.primary_key = primary_key
        self.null = null
        self.default = default
        self.unique = unique
        self.index = index
        self.validators = validators or []
        self.name = None
        self.auto_increment = auto_increment
        self.base_field = base_field
        self.to_model = to_model
        self.owner = None

    def validate(self, value: Any) -> None:
        """Template method for validation."""
        if value is None:
            if not self.null:
                raise DBFieldValidationError(f"Field {self.name} cannot be null")
            return
        self._validate_type(value)
        self._run_validators(value)

    def _validate_type(self, value: Any) -> None:
        """Hook for type-specific validation."""
        pass

    def _run_validators(self, value: Any) -> None:
        for validator in self.validators:
            try:
                validator(value)
            except Exception as e:
                raise DBFieldValidationError(
                    f"Validation failed for {self.name}: {str(e)}"
                )

    def sql_type(self) -> str:
        return self.type_mapper.get_sql_type(self.field_type)

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        return instance._data.get(self.name)

    def __set__(self, instance: Any, value: Any) -> None:
        if self.name not in instance._data:
            raise AttributeError(f"Cannot set undeclared field {self.name}")
        instance._data[self.name] = value

    def __eq__(self, other: Any) -> Any:
        if self.name and self.owner:
            self_table = self.owner.table_name()
            if isinstance(other, Field) and other.name and other.owner:
                other_table = other.owner.table_name()
                return F(f"{self_table}.{self.name} = {other_table}.{other.name}")
            else:
                # Handle comparison with literal values (e.g., int, str)
                return F(f"{self_table}.{self.name} = %s", [other])
        return False


class CharField(Field):
    def __init__(self, max_length: int = 255, **kwargs):
        super().__init__(field_type="str", max_length=max_length, **kwargs)
        self.max_length = max_length

    def _validate_type(self, value: Any) -> None:
        if not isinstance(value, str):
            raise DBFieldValidationError(f"Field {self.name} must be a string")
        if len(value) > self.max_length:
            raise DBFieldValidationError(
                f"Field {self.name} cannot exceed {self.max_length} characters"
            )


class TextField(Field):
    def __init__(self, **kwargs):
        super().__init__(field_type="text", **kwargs)

    def _validate_type(self, value: Any) -> None:
        if not isinstance(value, str):
            raise DBFieldValidationError(f"Field {self.name} must be a string")


class IntegerField(Field):
    def __init__(self, **kwargs):
        super().__init__(field_type="int", **kwargs)

    def _validate_type(self, value: Any) -> None:
        try:
            int(value)
        except (TypeError, ValueError):
            raise DBFieldValidationError(f"Field {self.name} must be an integer")


class FloatField(Field):
    def __init__(self, **kwargs):
        super().__init__(field_type="float", **kwargs)

    def _validate_type(self, value: Any) -> None:
        try:
            float(value)
        except (TypeError, ValueError):
            raise DBFieldValidationError(f"Field {self.name} must be a float")


class BooleanField(Field):
    def __init__(self, **kwargs):
        super().__init__(field_type="bool", **kwargs)

    def _validate_type(self, value: Any) -> None:
        if not isinstance(value, bool):
            raise DBFieldValidationError(f"Field {self.name} must be a boolean")


class DateTimeField(Field):
    def __init__(self, auto_now: bool = False, auto_now_add: bool = False, **kwargs):
        super().__init__(field_type="datetime", **kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def _validate_type(self, value: Any) -> None:
        if not isinstance(value, datetime):
            raise DBFieldValidationError(f"Field {self.name} must be a datetime object")


class DateField(Field):
    def __init__(self, auto_now: bool = False, auto_now_add: bool = False, **kwargs):
        super().__init__(field_type="date", **kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now_add

    def _validate_type(self, value: Any) -> None:
        if not isinstance(value, date):
            raise DBFieldValidationError(f"Field {self.name} must be a date object")


class JSONField(Field):
    def __init__(self, **kwargs):
        super().__init__(field_type="json", **kwargs)

    def _validate_type(self, value: Any) -> None:
        try:
            json.dumps(value)
        except (TypeError, ValueError):
            raise DBFieldValidationError(f"Field {self.name} must be JSON serializable")


class ArrayField(Field):
    def __init__(self, base_field: Field, **kwargs):
        super().__init__(field_type="array", base_field=base_field, **kwargs)
        self.base_field = base_field

    def _validate_type(self, value: Any) -> None:
        if not isinstance(value, (list, tuple)):
            raise DBFieldValidationError(f"Field {self.name} must be a list or tuple")
        for item in value:
            self.base_field.validate(item)

    def sql_type(self) -> str:
        return self.type_mapper.get_sql_type(
            self.field_type, base_field=self.base_field
        )


class DecimalField(Field):
    def __init__(self, max_digits: int = 10, decimal_places: int = 2, **kwargs):
        super().__init__(
            field_type="decimal",
            max_digits=max_digits,
            decimal_places=decimal_places,
            **kwargs,
        )
        self.max_digits = max_digits
        self.decimal_places = decimal_places

    def _validate_type(self, value: Any) -> None:
        try:
            decimal_value = Decimal(str(value))
            total_digits = len(decimal_value.as_tuple().digits)
            if total_digits > self.max_digits:
                raise DBFieldValidationError(
                    f"Field {self.name} exceeds maximum digits {self.max_digits}"
                )
            if decimal_value.as_tuple().exponent < -self.decimal_places:
                raise DBFieldValidationError(
                    f"Field {self.name} exceeds maximum decimal places {self.decimal_places}"
                )
        except (InvalidOperation, ValueError):
            raise DBFieldValidationError(
                f"Field {self.name} must be a valid decimal number"
            )

    def sql_type(self) -> str:
        return self.type_mapper.get_sql_type(
            self.field_type,
            max_digits=self.max_digits,
            decimal_places=self.decimal_places,
        )


class ForeignKeyField(Field):
    def __init__(
        self,
        to_model: Union[str, Any],
        related_field: str = "id",
        field_type: str = "int",
        on_delete: str = "CASCADE",
        on_update: str = "CASCADE",
        related_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(field_type=field_type, **kwargs)
        self.to_model = to_model
        self.related_field = related_field
        self.on_delete = on_delete.upper()
        self.on_update = on_update.upper()
        self.related_name = related_name

        valid_actions = {"CASCADE", "SET NULL", "RESTRICT", "NO ACTION"}
        if self.on_delete not in valid_actions:
            raise ValueError(
                f"Invalid on_delete action. Must be one of: {valid_actions}"
            )
        if self.on_update not in valid_actions:
            raise ValueError(
                f"Invalid on_update action. Must be one of: {valid_actions}"
            )

        if (
            self.on_delete == "SET NULL" or self.on_update == "SET NULL"
        ) and not kwargs.get("null", True):
            raise ValueError(
                "Field must be nullable to use SET NULL referential action"
            )

    def __get__(self, instance: Any, owner: Any) -> Any:
        if instance is None:
            return self
        if self.name in instance._related_data:
            return instance._related_data.get(self.name)
        fk_value = instance._data.get(self.name)
        if fk_value is None:
            return None
        # Fetch the related model instance
        related_model = (
            self.to_model
            if not isinstance(self.to_model, str)
            else self.owner._fields[self.name].to_model
        )
        try:
            related_instance = (
                related_model.objects().where(**{self.related_field: fk_value}).get()
            )
            instance._related_data[self.name] = related_instance
            return related_instance
        except DoesNotExist:
            instance._related_data[self.name] = None
            return None

    def __set__(self, instance: Any, value: Any) -> None:
        if self.name not in instance._data:
            raise AttributeError(f"Cannot set undeclared field {self.name}")
        if isinstance(value, self.to_model):
            instance._data[self.name] = value._data.get(self.related_field)
            instance._related_data[self.name] = value
        else:
            instance._data[self.name] = value
            if self.name in instance._related_data:
                del instance._related_data[self.name]

    def __eq__(self, other: Any) -> Any:
        if self.name and self.owner:
            self_table = self.owner.table_name()
            if isinstance(other, Field) and other.name:
                other_table = self.to_model.table_name()
                return F(f"{self_table}.{self.name} = {other_table}.{other.name}")
            else:
                return F(f"{self_table}.{self.name} = %s", [other])
        return False

    def _validate_type(self, value: Any) -> None:
        if isinstance(self.to_model, str):
            return
        related_field_obj = getattr(self.to_model, self.related_field)
        try:
            related_field_obj.validate(value)
        except DBFieldValidationError as e:
            raise DBFieldValidationError(
                f"Foreign key {self.name} validation failed: {str(e)}"
            )
