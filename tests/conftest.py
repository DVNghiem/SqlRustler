"""
Pytest configuration and fixtures for SqlRustler tests.
"""
import pytest


@pytest.fixture
def sample_database_config():
    """Provide a sample database configuration for testing."""
    from sqlrustler import DatabaseConfig, DatabaseType
    
    return DatabaseConfig(
        driver=DatabaseType.Sqlite,
        url="sqlite::memory:",
        max_connections=5,
        min_connections=1,
        idle_timeout=30,
    )


@pytest.fixture
def mock_model():
    """Create a simple mock model for testing."""
    from sqlrustler import Model, IntegerField, TextField
    
    class TestModel(Model):
        __tablename__ = "test_table"
        id = IntegerField(primary_key=True)
        name = TextField()
        description = TextField(null=True)
    
    return TestModel


@pytest.fixture
def mock_related_models():
    """Create related models for testing ForeignKey relationships."""
    from sqlrustler import Model, IntegerField, TextField, ForeignKeyField
    
    class Company(Model):
        __tablename__ = "companies"
        id = IntegerField(primary_key=True)
        name = TextField()
    
    class Employee(Model):
        __tablename__ = "employees"
        id = IntegerField(primary_key=True)
        name = TextField()
        company_id = ForeignKeyField(Company, related_field="id")
    
    return {"Company": Company, "Employee": Employee}
