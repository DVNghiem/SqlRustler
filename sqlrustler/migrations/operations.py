"""
Migration operations for SqlRustler.
"""
from typing import Any, Dict, List, Optional
from sqlrustler.field import Field


class Operation:
    """Base class for migration operations."""
    
    def to_sql(self, schema_editor) -> str:
        raise NotImplementedError


class CreateTable(Operation):
    def __init__(self, name: str, fields: Dict[str, Field]):
        self.name = name
        self.fields = fields

    def to_sql(self, schema_editor) -> str:
        return schema_editor.create_table_sql(self.name, self.fields)


class DeleteTable(Operation):
    def __init__(self, name: str):
        self.name = name

    def to_sql(self, schema_editor) -> str:
        return schema_editor.delete_table_sql(self.name)


class AddColumn(Operation):
    def __init__(self, model_name: str, name: str, field: Field):
        self.model_name = model_name
        self.name = name
        self.field = field

    def to_sql(self, schema_editor) -> str:
        return schema_editor.add_column_sql(self.model_name, self.name, self.field)


class RemoveColumn(Operation):
    def __init__(self, model_name: str, name: str):
        self.model_name = model_name
        self.name = name

    def to_sql(self, schema_editor) -> str:
        return schema_editor.remove_column_sql(self.model_name, self.name)


class AlterColumn(Operation):
    def __init__(self, model_name: str, name: str, field: Field):
        self.model_name = model_name
        self.name = name
        self.field = field

    def to_sql(self, schema_editor) -> str:
        return schema_editor.alter_column_sql(self.model_name, self.name, self.field)
