"""
Test suite for SqlRustler ORM Model functionality.
"""
import pytest
from sqlrustler.model import Model, to_snake_case
from sqlrustler.field import IntegerField, TextField, ForeignKeyField


class TestSnakeCaseConversion:
    """Test snake_case conversion utility."""
    
    def test_camel_case_to_snake_case(self):
        """Test CamelCase to snake_case conversion."""
        assert to_snake_case("CamelCase") == "camel_case"
        assert to_snake_case("HTTPResponse") == "h_t_t_p_response"
        assert to_snake_case("SimpleModel") == "simple_model"
    
    def test_already_snake_case(self):
        """Test already snake_case strings."""
        assert to_snake_case("already_snake") == "already_snake"
        assert to_snake_case("test") == "test"


class TestModelDefinition:
    """Test Model class definition and metadata."""
    
    def test_model_table_name_explicit(self):
        """Test model with explicit table name."""
        class TestModel(Model):
            __tablename__ = "custom_table"
            id = IntegerField(primary_key=True)
            name = TextField()
        
        assert TestModel.table_name() == "custom_table"
    
    def test_model_table_name_auto(self):
        """Test model with auto-generated table name."""
        class UserProfile(Model):
            id = IntegerField(primary_key=True)
            name = TextField()
        
        assert UserProfile.table_name() == "user_profile"
    
    def test_model_fields_registration(self):
        """Test that fields are properly registered."""
        class TestModel(Model):
            id = IntegerField(primary_key=True)
            name = TextField()
            email = TextField()
        
        assert "id" in TestModel._fields
        assert "name" in TestModel._fields
        assert "email" in TestModel._fields
        assert len(TestModel._fields) == 3


class TestModelForeignKey:
    """Test ForeignKey SQL generation."""
    
    def test_foreign_key_sql_generation(self):
        """Test that foreign key SQL uses table_name() method."""
        class Company(Model):
            __tablename__ = "companies"
            id = IntegerField(primary_key=True)
            name = TextField()
        
        class Employee(Model):
            __tablename__ = "employees"
            id = IntegerField(primary_key=True)
            name = TextField()
            company_id = ForeignKeyField(Company, related_field="id")
        
        # Get the foreign key SQL
        fk_sql = Employee._get_foreign_key_sql("company_id", Employee._fields["company_id"])
        
        # Should use "companies" (from table_name()) not "company" (from __name__.lower())
        assert "companies" in fk_sql
        assert "FOREIGN KEY (company_id) REFERENCES companies(id)" in fk_sql


class TestModelInstance:
    """Test Model instance creation and data handling."""
    
    def test_model_instance_creation(self):
        """Test creating model instances."""
        class User(Model):
            id = IntegerField(primary_key=True)
            name = TextField()
            email = TextField()
        
        user = User(id=1, name="John Doe", email="john@example.com")
        assert user._data["id"] == 1
        assert user._data["name"] == "John Doe"
        assert user._data["email"] == "john@example.com"
    
    def test_model_instance_unknown_field(self):
        """Test that unknown fields raise ValueError."""
        class User(Model):
            id = IntegerField(primary_key=True)
            name = TextField()
        
        with pytest.raises(ValueError, match="Unknown field"):
            User(id=1, name="John", invalid_field="value")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
