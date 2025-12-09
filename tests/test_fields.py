"""
Test suite for SqlRustler ORM field functionality.
"""
import pytest
from sqlrustler.field import (
    Field, CharField, TextField, IntegerField, FloatField,
    BooleanField, DateTimeField, DateField, JSONField,
    ArrayField, DecimalField, ForeignKeyField,
    PostgresTypeMapper, MySqlTypeMapper
)
from sqlrustler.exceptions import DBFieldValidationError
from datetime import datetime, date
from decimal import Decimal


class TestFieldTypeMapper:
    """Test Field type_mapper initialization and SQL type generation."""
    
    def test_field_type_mapper_initialization(self):
        """Test that type_mapper is properly initialized."""
        field = IntegerField()
        # Should not raise AttributeError
        assert field.type_mapper is not None
        assert isinstance(field.type_mapper, (PostgresTypeMapper, MySqlTypeMapper))
    
    def test_field_sql_type_generation(self):
        """Test SQL type generation doesn't raise errors."""
        field = IntegerField()
        sql_type = field.sql_type()
        assert sql_type == "INTEGER"
    
    def test_char_field_with_max_length(self):
        """Test CharField properly passes max_length to type mapper."""
        field = CharField(max_length=100)
        sql_type = field.sql_type()
        assert "100" in sql_type or sql_type == "VARCHAR(100)"
    
    def test_decimal_field_with_precision(self):
        """Test DecimalField properly passes precision parameters."""
        field = DecimalField(max_digits=10, decimal_places=2)
        sql_type = field.sql_type()
        assert "DECIMAL" in sql_type


class TestFieldValidation:
    """Test field validation logic."""
    
    def test_integer_field_validation(self):
        """Test IntegerField validates integers correctly."""
        field = IntegerField(name="test_field")
        field.validate(42)  # Should not raise
        field.validate("42")  # Should convert and validate
        
    def test_integer_field_validation_fails(self):
        """Test IntegerField rejects invalid values."""
        field = IntegerField(name="test_field", null=False)
        with pytest.raises(DBFieldValidationError):
            field.validate("not_a_number")
    
    def test_char_field_max_length_validation(self):
        """Test CharField enforces max_length."""
        field = CharField(max_length=10, name="test_field")
        field.validate("short")  # Should not raise
        
        with pytest.raises(DBFieldValidationError):
            field.validate("this is way too long for the field")
    
    def test_boolean_field_validation(self):
        """Test BooleanField validates booleans."""
        field = BooleanField(name="test_field")
        field.validate(True)  # Should not raise
        field.validate(False)  # Should not raise
        
        with pytest.raises(DBFieldValidationError):
            field.validate("not a boolean")
    
    def test_datetime_field_validation(self):
        """Test DateTimeField validates datetime objects."""
        field = DateTimeField(name="test_field")
        field.validate(datetime.now())  # Should not raise
        
        with pytest.raises(DBFieldValidationError):
            field.validate("2024-01-01")  # String not allowed
    
    def test_json_field_validation(self):
        """Test JSONField validates JSON-serializable data."""
        field = JSONField(name="test_field")
        field.validate({"key": "value"})  # Should not raise
        field.validate([1, 2, 3])  # Should not raise
        
        # Objects that can't be JSON serialized should fail
        with pytest.raises(DBFieldValidationError):
            field.validate(object())
    
    def test_decimal_field_validation(self):
        """Test DecimalField validates decimal values."""
        field = DecimalField(max_digits=5, decimal_places=2, name="test_field")
        field.validate(Decimal("123.45"))  # Should not raise
        field.validate(123.45)  # Should convert and validate
        
        with pytest.raises(DBFieldValidationError):
            field.validate(Decimal("123456.78"))  # Too many digits
    
    def test_null_validation(self):
        """Test null validation works correctly."""
        field = IntegerField(null=False, name="test_field")
        
        with pytest.raises(DBFieldValidationError):
            field.validate(None)
        
        nullable_field = IntegerField(null=True, name="test_field")
        nullable_field.validate(None)  # Should not raise


class TestForeignKeyField:
    """Test ForeignKey field functionality."""
    
    def test_foreign_key_field_creation(self):
        """Test ForeignKeyField can be created."""
        field = ForeignKeyField(to_model="Company", related_field="id")
        assert field.to_model == "Company"
        assert field.related_field == "id"
        assert field.on_delete == "CASCADE"
    
    def test_foreign_key_invalid_on_delete(self):
        """Test ForeignKeyField rejects invalid on_delete values."""
        with pytest.raises(ValueError):
            ForeignKeyField(to_model="Company", on_delete="INVALID")
    
    def test_foreign_key_set_null_requires_nullable(self):
        """Test SET NULL requires null=True."""
        with pytest.raises(ValueError):
            ForeignKeyField(to_model="Company", on_delete="SET NULL", null=False)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
