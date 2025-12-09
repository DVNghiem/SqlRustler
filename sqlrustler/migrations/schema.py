"""
Schema editor for generating SQL from migration operations.
"""
from typing import Dict, Any
from sqlrustler.field import Field, ForeignKeyField
from sqlrustler.model import Model


class SchemaEditor:
    def __init__(self, connection=None):
        self.connection = connection

    def create_table_sql(self, table_name: str, fields: Dict[str, Field]) -> str:
        field_defs = []
        constraints = []
        
        for name, field in fields.items():
            field.name = name  # Ensure name is set
            # Use the field's sql_type method which now works correctly
            # We need to mock the alias/owner if not present for type mapping
            if not field.alias:
                field.alias = "default" 
                
            definition = f"{name} {field.sql_type()}"
            
            if field.primary_key:
                definition += " PRIMARY KEY"
            if field.auto_increment:
                 # Simple heuristic for now, ideally depends on DB type
                definition += " AUTO_INCREMENT" if "INTEGER" in field.sql_type() else " SERIAL"
            if not field.null:
                definition += " NOT NULL"
            if field.unique:
                definition += " UNIQUE"
            if field.default is not None:
                default_val = f"'{field.default}'" if isinstance(field.default, str) else str(field.default)
                definition += f" DEFAULT {default_val}"
                
            field_defs.append(definition)
            
            if isinstance(field, ForeignKeyField):
                # We need to handle the target table name resolution
                # This is a bit tricky without the full model registry
                # For now, we assume to_model is a string or we can get the table name
                target = field.to_model
                if not isinstance(target, str):
                    target = target.table_name()
                else:
                    target = target.lower() # Fallback
                    
                constraints.append(
                    f"FOREIGN KEY ({name}) REFERENCES {target}({field.related_field}) "
                    f"ON DELETE {field.on_delete} ON UPDATE {field.on_update}"
                )

        full_defs = field_defs + constraints
        return f"CREATE TABLE {table_name} (\n    " + ",\n    ".join(full_defs) + "\n);"

    def delete_table_sql(self, table_name: str) -> str:
        return f"DROP TABLE IF EXISTS {table_name};"

    def add_column_sql(self, table_name: str, name: str, field: Field) -> str:
        field.name = name
        if not field.alias:
            field.alias = "default"
            
        definition = f"{name} {field.sql_type()}"
        if not field.null:
            definition += " NOT NULL"
        if field.default is not None:
             default_val = f"'{field.default}'" if isinstance(field.default, str) else str(field.default)
             definition += f" DEFAULT {default_val}"
             
        return f"ALTER TABLE {table_name} ADD COLUMN {definition};"

    def remove_column_sql(self, table_name: str, name: str) -> str:
        return f"ALTER TABLE {table_name} DROP COLUMN {name};"

    def alter_column_sql(self, table_name: str, name: str, field: Field) -> str:
        # This is highly DB specific, implementing a generic version
        # Postgres uses TYPE, MySQL uses MODIFY
        # For now, let's assume a generic MODIFY/TYPE syntax or just implement for one
        # Let's default to a generic "ALTER COLUMN" which works for Postgres
        
        field.name = name
        if not field.alias:
            field.alias = "default"
            
        return f"ALTER TABLE {table_name} ALTER COLUMN {name} TYPE {field.sql_type()};"
