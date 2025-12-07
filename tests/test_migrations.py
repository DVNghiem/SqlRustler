"""
Test cases for SqlRustler migration system.
"""
import pytest
import os
import tempfile
import shutil
from pathlib import Path


class TestOperations:
    """Test migration operation classes."""
    
    def test_create_table_operation(self):
        from sqlrustler.migrations.operations import CreateTable
        from sqlrustler.field import IntegerField, CharField
        
        fields = {
            'id': IntegerField(primary_key=True),
            'name': CharField(max_length=100)
        }
        
        op = CreateTable(name='test_table', fields=fields)
        assert op.name == 'test_table'
        assert len(op.fields) == 2
        assert 'id' in op.fields
        assert 'name' in op.fields
    
    def test_delete_table_operation(self):
        from sqlrustler.migrations.operations import DeleteTable
        
        op = DeleteTable(name='test_table')
        assert op.name == 'test_table'
    
    def test_add_column_operation(self):
        from sqlrustler.migrations.operations import AddColumn
        from sqlrustler.field import CharField
        
        field = CharField(max_length=50)
        op = AddColumn(model_name='test_table', name='email', field=field)
        
        assert op.model_name == 'test_table'
        assert op.name == 'email'
        assert op.field is not None
    
    def test_remove_column_operation(self):
        from sqlrustler.migrations.operations import RemoveColumn
        
        op = RemoveColumn(model_name='test_table', name='email')
        assert op.model_name == 'test_table'
        assert op.name == 'email'


class TestSchemaEditor:
    """Test SQL generation from operations."""
    
    def test_create_table_sql(self):
        from sqlrustler.migrations.schema import SchemaEditor
        from sqlrustler.field import IntegerField, CharField
        
        editor = SchemaEditor()
        fields = {
            'id': IntegerField(primary_key=True),
            'name': CharField(max_length=100, null=False)
        }
        
        sql = editor.create_table_sql('users', fields)
        
        assert 'CREATE TABLE users' in sql
        assert 'id' in sql
        assert 'PRIMARY KEY' in sql
        assert 'name' in sql
        assert 'NOT NULL' in sql
    
    def test_delete_table_sql(self):
        from sqlrustler.migrations.schema import SchemaEditor
        
        editor = SchemaEditor()
        sql = editor.delete_table_sql('old_table')
        
        assert 'DROP TABLE' in sql
        assert 'old_table' in sql
    
    def test_add_column_sql(self):
        from sqlrustler.migrations.schema import SchemaEditor
        from sqlrustler.field import CharField
        
        editor = SchemaEditor()
        field = CharField(max_length=100, null=True)
        sql = editor.add_column_sql('users', 'email', field)
        
        assert 'ALTER TABLE users' in sql
        assert 'ADD COLUMN email' in sql
    
    def test_remove_column_sql(self):
        from sqlrustler.migrations.schema import SchemaEditor
        
        editor = SchemaEditor()
        sql = editor.remove_column_sql('users', 'old_field')
        
        assert 'ALTER TABLE users' in sql
        assert 'DROP COLUMN old_field' in sql
    
    def test_create_table_with_foreign_key(self):
        from sqlrustler.migrations.schema import SchemaEditor
        from sqlrustler.field import IntegerField, ForeignKeyField
        from sqlrustler import Model
        
        class Company(Model):
            __tablename__ = 'companies'
            id = IntegerField(primary_key=True)
        
        editor = SchemaEditor()
        fields = {
            'id': IntegerField(primary_key=True),
            'company_id': ForeignKeyField(to_model='companies', related_field='id')
        }
        
        sql = editor.create_table_sql('employees', fields)
        
        assert 'CREATE TABLE employees' in sql
        assert 'FOREIGN KEY' in sql
        assert 'REFERENCES companies' in sql


class TestAutoDetector:
    """Test automatic detection of schema changes."""
    
    def test_detect_new_model(self):
        from sqlrustler.migrations.autodetector import AutoDetector
        from sqlrustler import Model, IntegerField, CharField
        
        class NewModel(Model):
            __tablename__ = 'new_table'
            id = IntegerField(primary_key=True)
            name = CharField(max_length=100)
        
        old_state = {}
        new_state = {'NewModel': NewModel}
        
        detector = AutoDetector(old_state, new_state)
        operations = detector.detect_changes()
        
        assert len(operations) == 1
        assert operations[0].name == 'new_table'
    
    def test_no_changes(self):
        from sqlrustler.migrations.autodetector import AutoDetector
        from sqlrustler import Model, IntegerField
        
        class ExistingModel(Model):
            __tablename__ = 'existing'
            id = IntegerField(primary_key=True)
        
        state = {'ExistingModel': ExistingModel}
        
        detector = AutoDetector(state, state)
        operations = detector.detect_changes()
        
        assert len(operations) == 0
    
    def test_detect_multiple_new_models(self):
        from sqlrustler.migrations.autodetector import AutoDetector
        from sqlrustler import Model, IntegerField, CharField
        
        class Model1(Model):
            __tablename__ = 'table1'
            id = IntegerField(primary_key=True)
        
        class Model2(Model):
            __tablename__ = 'table2'
            id = IntegerField(primary_key=True)
            name = CharField(max_length=50)
        
        old_state = {}
        new_state = {'Model1': Model1, 'Model2': Model2}
        
        detector = AutoDetector(old_state, new_state)
        operations = detector.detect_changes()
        
        assert len(operations) == 2


class TestMigrationWriter:
    """Test migration file generation."""
    
    def test_basic_migration_file(self):
        from sqlrustler.migrations.writer import MigrationWriter
        from sqlrustler.migrations.operations import CreateTable
        from sqlrustler.field import IntegerField, CharField
        
        fields = {
            'id': IntegerField(primary_key=True),
            'name': CharField(max_length=100)
        }
        
        op = CreateTable(name='test_table', fields=fields)
        writer = MigrationWriter('test_migration', [op])
        content = writer.as_string()
        
        assert 'Generated by SqlRustler' in content
        assert 'from sqlrustler.migrations.operations import *' in content
        assert 'from sqlrustler.field import *' in content
        assert 'CreateTable' in content
        assert 'test_table' in content
    
    def test_migration_with_dependencies(self):
        from sqlrustler.migrations.writer import MigrationWriter
        from sqlrustler.migrations.operations import CreateTable
        from sqlrustler.field import IntegerField
        
        fields = {'id': IntegerField(primary_key=True)}
        op = CreateTable(name='test', fields=fields)
        
        writer = MigrationWriter('test', [op], dependencies=['0001_initial'])
        content = writer.as_string()
        
        assert "dependencies = ['0001_initial']" in content


class TestMigrationLoader:
    """Test loading migration files."""
    
    def test_load_migrations_from_directory(self, tmp_path):
        from sqlrustler.migrations.loader import MigrationLoader
        
        # Create a temporary migrations directory
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        
        # Create __init__.py
        (migrations_dir / "__init__.py").write_text("")
        
        # Create a test migration file
        migration_content = '''
from sqlrustler.migrations.operations import *
from sqlrustler.field import *

dependencies = []

operations = [
    CreateTable(name='test', fields={'id': IntegerField(primary_key=True)})
]
'''
        (migrations_dir / "0001_test.py").write_text(migration_content)
        
        loader = MigrationLoader(migration_dir=str(migrations_dir))
        migrations = loader.load_migrations()
        
        assert len(migrations) == 1
        assert migrations[0][0] == '0001_test'
        assert hasattr(migrations[0][1], 'operations')
    
    def test_empty_migrations_directory(self, tmp_path):
        from sqlrustler.migrations.loader import MigrationLoader
        
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        
        loader = MigrationLoader(migration_dir=str(migrations_dir))
        migrations = loader.load_migrations()
        
        assert len(migrations) == 0
    
    def test_nonexistent_directory(self):
        from sqlrustler.migrations.loader import MigrationLoader
        
        loader = MigrationLoader(migration_dir="/nonexistent/path")
        migrations = loader.load_migrations()
        
        assert len(migrations) == 0


class TestMigrationManager:
    """Test migration manager functionality."""
    
    def test_get_all_models(self, tmp_path, monkeypatch):
        """Test model discovery from models.py"""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)
        
        # Create a models.py file
        models_content = '''
from sqlrustler import Model, IntegerField, CharField

class TestModel(Model):
    __tablename__ = 'test'
    id = IntegerField(primary_key=True)
    name = CharField(max_length=50)
'''
        (tmp_path / "models.py").write_text(models_content)
        
        from sqlrustler.migrations.manager import get_all_models
        models = get_all_models()
        
        assert 'TestModel' in models
        assert hasattr(models['TestModel'], '_fields')
    
    def test_makemigrations_creates_file(self, tmp_path, monkeypatch):
        """Test that makemigrations creates a migration file"""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)
        
        # Create a models.py file
        models_content = '''
from sqlrustler import Model, IntegerField, CharField

class User(Model):
    __tablename__ = 'users'
    id = IntegerField(primary_key=True)
    username = CharField(max_length=50)
'''
        (tmp_path / "models.py").write_text(models_content)
        
        from sqlrustler.migrations.manager import makemigrations
        
        # Run makemigrations
        makemigrations("test_migration")
        
        # Check that migrations directory was created
        migrations_dir = tmp_path / "migrations"
        assert migrations_dir.exists()
        
        # Check that a migration file was created
        migration_files = list(migrations_dir.glob("*_test_migration.py"))
        assert len(migration_files) == 1
        
        # Read and verify content
        content = migration_files[0].read_text()
        assert 'CreateTable' in content
        assert 'users' in content
    
    def test_makemigrations_no_changes(self, tmp_path, monkeypatch, capsys):
        """Test makemigrations when there are no changes"""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)
        
        # Create empty models.py
        (tmp_path / "models.py").write_text("from sqlrustler import Model\n")
        
        from sqlrustler.migrations.manager import makemigrations
        makemigrations("test")
        
        captured = capsys.readouterr()
        assert "No changes detected" in captured.out


class TestMigrationExecutor:
    """Test migration execution."""
    
    @pytest.fixture
    def test_db_session(self):
        """Create an in-memory SQLite database for testing"""
        from sqlrustler import DatabaseConfig, DatabaseType, DatabaseConnection
        import uuid
        
        # Use a unique alias for each test to avoid conflicts
        alias = f"test_db_{uuid.uuid4().hex[:8]}"
        
        config = DatabaseConfig(
            driver=DatabaseType.Sqlite,
            url="sqlite::memory:",
            max_connections=1,
            min_connections=1,
            idle_timeout=30,
        )
        
        DatabaseConnection.connect(config, alias)
        yield alias
    
    def test_ensure_migrations_table(self, test_db_session):
        """Test that the migrations table is created"""
        from sqlrustler.migrations.executor import MigrationExecutor
        from sqlrustler import Session
        
        executor = MigrationExecutor(connection_alias=test_db_session)
        
        with Session(alias=test_db_session) as tx:
            from sqlrustler import DatabaseType
            executor._ensure_migrations_table(tx, DatabaseType.Sqlite)
            
            # Verify table exists
            result = tx.fetch_all(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='sqlrustler_migrations'",
                []
            )
            assert len(result) > 0
    
    def test_record_and_get_applied_migrations(self, test_db_session):
        """Test recording and retrieving applied migrations"""
        from sqlrustler.migrations.executor import MigrationExecutor
        from sqlrustler import Session, DatabaseType
        
        executor = MigrationExecutor(connection_alias=test_db_session)
        
        with Session(alias=test_db_session) as tx:
            executor._ensure_migrations_table(tx, DatabaseType.Sqlite)
            
            # Record a migration
            executor._record_migration(tx, "0001_initial")
            
            # Get applied migrations
            applied = executor._get_applied_migrations(tx)
            assert "0001_initial" in applied
    
    def test_migrate_with_operations(self, tmp_path, test_db_session, monkeypatch):
        """Test full migration execution"""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)
        
        # Create migrations directory
        migrations_dir = tmp_path / "migrations"
        migrations_dir.mkdir()
        (migrations_dir / "__init__.py").write_text("")
        
        # Create a migration file
        migration_content = '''
from sqlrustler.migrations.operations import CreateTable
from sqlrustler.field import IntegerField, CharField

dependencies = []

operations = [
    CreateTable(name='test_users', fields={
        'id': IntegerField(primary_key=True),
        'username': CharField(max_length=50)
    })
]
'''
        (migrations_dir / "0001_initial.py").write_text(migration_content)
        
        from sqlrustler.migrations.executor import MigrationExecutor
        from sqlrustler import Session
        
        executor = MigrationExecutor(connection_alias=test_db_session)
        executor.migrate()
        
        # Verify table was created
        with Session(alias=test_db_session) as tx:
            result = tx.fetch_all(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='test_users'",
                []
            )
            assert len(result) > 0


class TestEndToEndMigration:
    """End-to-end integration tests for the migration system."""
    
    def test_full_migration_workflow(self, tmp_path, monkeypatch):
        """Test complete workflow: model -> makemigrations -> migrate"""
        # Change to temp directory
        monkeypatch.chdir(tmp_path)
        
        # Step 1: Create models.py
        models_content = '''
from sqlrustler import Model, IntegerField, CharField

class Article(Model):
    __tablename__ = 'articles'
    id = IntegerField(primary_key=True)
    title = CharField(max_length=200)
    content = CharField(max_length=1000)
'''
        (tmp_path / "models.py").write_text(models_content)
        
        # Step 2: Run makemigrations
        from sqlrustler.migrations.manager import makemigrations
        makemigrations("create_articles")
        
        # Step 3: Verify migration file was created
        migrations_dir = tmp_path / "migrations"
        assert migrations_dir.exists()
        migration_files = list(migrations_dir.glob("*_create_articles.py"))
        assert len(migration_files) == 1
        
        # Step 4: Verify migration file content
        content = migration_files[0].read_text()
        assert 'CreateTable' in content
        assert 'articles' in content
        assert 'title' in content
        assert 'content' in content
        
        # Step 5: Setup test database
        from sqlrustler import DatabaseConfig, DatabaseType, DatabaseConnection
        config = DatabaseConfig(
            driver=DatabaseType.Sqlite,
            url="sqlite::memory:",
            max_connections=1,
            min_connections=1,
            idle_timeout=30,
        )
        DatabaseConnection.connect(config, "test_workflow")
        
        # Step 6: Run migrate
        from sqlrustler.migrations.executor import MigrationExecutor
        from sqlrustler import Session
        
        executor = MigrationExecutor(connection_alias="test_workflow")
        executor.migrate()
        
        # Step 7: Verify table was created
        with Session(alias="test_workflow") as tx:
            result = tx.fetch_all(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='articles'",
                []
            )
            assert len(result) > 0
