import re
from datetime import date, datetime
from typing import Optional

from .field import Field, ForeignKeyField
from .query import QuerySet
from .sqlrustler import Session



def to_snake_case(name: str) -> str:
    """Convert CamelCase to snake_case."""
    return re.sub("(?!^)([A-Z])", r"_\1", name).lower()


class MetaModel(type):
    def __new__(mcs, name, bases, attrs):
        if name == "Model" and not bases:
            return super().__new__(mcs, name, bases, attrs)

        fields = {}
        table_name = attrs.get("__tablename__")
        alias = attrs.get("__alias__", "default")

        if not table_name:
            table_name = to_snake_case(name)

        for key, value in list(attrs.items()):
            if isinstance(value, Field):
                value.alias = alias
                fields[key] = value
                value.name = key

        attrs["_fields"] = fields
        attrs["_table_name"] = table_name
        attrs["_alias"] = alias

        return super().__new__(mcs, name, bases, attrs)


class Model(metaclass=MetaModel):
    _connection = None
    _alias = "default"

    @classmethod
    def get_session(cls, alias: Optional[str] = None) -> Session:
        """Get a session for query execution."""
        return Session(alias=alias or cls._alias)

    def __init__(self, **kwargs):
        self._data = {}
        for name, field in self._fields.items():
            if field.default is not None:
                self._data[name] = field.default
            if name in kwargs:
                self._data[name] = kwargs[name]

    @classmethod
    def objects(cls, alias: Optional[str] = None) -> QuerySet:
        return QuerySet(cls, alias=alias or cls._alias)

    @classmethod
    def table_name(cls) -> str:
        return cls._table_name

    @classmethod
    def create_table_sql(cls) -> str:
        fields_sql = []
        indexes_sql = []
        foreign_keys = []

        for name, field in cls._fields.items():
            fields_sql.append(cls._get_field_sql(name, field))
            if field.index:
                indexes_sql.append(cls._get_index_sql(name))
            if isinstance(field, ForeignKeyField):
                foreign_keys.append(cls._get_foreign_key_sql(name, field))

        fields_sql.extend(foreign_keys)
        joined_fields_sql = ", \n ".join(fields_sql)

        create_table = f"CREATE TABLE {cls.table_name()} (\n  {joined_fields_sql} \n)"

        return f"{create_table};\n" + ";\n".join(indexes_sql)

    @classmethod
    def _get_field_sql(cls, name, field) -> str:
        field_def = [f"{name} {field.sql_type()}"]
        if field.primary_key:
            field_def.append("PRIMARY KEY")
        if field.auto_increment and field.sql_type().lower() in ("integer", "int"):
            field_def.append("AUTO_INCREMENT")
        if not field.null:
            field_def.append("NOT NULL")
        if field.unique:
            field_def.append("UNIQUE")
        if field.default is not None:
            if isinstance(field.default, (str, datetime, date)):
                field_def.append(f"DEFAULT '{field.default}'")
            else:
                field_def.append(f"DEFAULT {field.default}")
        return " ".join(field_def)

    @classmethod
    def _get_index_sql(cls, name) -> str:
        return f"CREATE INDEX idx_{cls.table_name()}_{name} ON {cls.table_name()} ({name})"

    @classmethod
    def _get_foreign_key_sql(cls, name, field) -> str:
        target_table = field.to_model.__name__.lower() if not isinstance(field.to_model, str) else field.to_model.lower()
        return f"FOREIGN KEY ({name}) REFERENCES {target_table}({field.related_field}) ON DELETE {field.on_delete} ON UPDATE {field.on_update}"

    def save(self):
        for name, value in self._data.items():
            self._fields[name].validate(value)
        query_object = QuerySet(self, alias=self._alias)
        query_object.bulk_create([self])