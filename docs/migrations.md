# Database Migrations

Migrations allow you to version control your database schema and make incremental changes to the database structure.

## Creating Migrations

### Automatic Migration Detection

SqlRustler can detect changes to your models and create migrations automatically:

```bash
# Detect changes and create migration
python manage.py makemigrations

# Create an empty migration with a specific name
python manage.py makemigrations --name add_user_fields
```

### Migration File Structure

Migrations are stored in the `migrations/` directory:

```
migrations/
├── __init__.py
├── 1764179025_initial.py
├── 1764179100_add_fields.py
└── ...
```

Each migration file contains operations that modify the database:

```python
from sqlrustler.migrations.operations import (
    CreateModel, AddField, RemoveField, AlterField
)

def up(schema_editor):
    """Apply migration"""
    schema_editor.create_model(
        "User",
        fields=[
            ("id", "IntegerField", {"primary_key": True}),
            ("name", "TextField", {"max_length": 255}),
        ]
    )

def down(schema_editor):
    """Reverse migration"""
    schema_editor.delete_model("User")
```

## Running Migrations

### Apply All Migrations

```bash
# Apply pending migrations
python manage.py migrate

# Apply migrations to specific app
python manage.py migrate myapp
```

### Migrate to Specific Point

```bash
# Apply up to specific migration
python manage.py migrate 1764179100

# Rollback to specific migration
python manage.py migrate 1764179025
```

### Check Migration Status

```bash
# Show migration status
python manage.py showmigrations
```

## Common Migration Operations

### Create a New Table

```python
def up(schema_editor):
    schema_editor.create_model(
        "Article",
        fields=[
            ("id", "IntegerField", {"primary_key": True}),
            ("title", "TextField", {"max_length": 200}),
            ("content", "TextField", {}),
            ("created_at", "DateTimeField", {}),
        ]
    )

def down(schema_editor):
    schema_editor.delete_model("Article")
```

### Add a Column

```python
def up(schema_editor):
    schema_editor.add_field(
        "User",
        "bio",
        "TextField",
        {"null": True, "default": ""}
    )

def down(schema_editor):
    schema_editor.remove_field("User", "bio")
```

### Remove a Column

```python
def up(schema_editor):
    schema_editor.remove_field("User", "legacy_field")

def down(schema_editor):
    schema_editor.add_field(
        "User",
        "legacy_field",
        "TextField",
        {}
    )
```

### Rename a Column

```python
def up(schema_editor):
    schema_editor.rename_field("User", "old_name", "new_name")

def down(schema_editor):
    schema_editor.rename_field("User", "new_name", "old_name")
```

### Modify a Column

```python
def up(schema_editor):
    schema_editor.alter_field(
        "User",
        "email",
        "TextField",
        {"max_length": 500, "unique": True}
    )

def down(schema_editor):
    schema_editor.alter_field(
        "User",
        "email",
        "TextField",
        {"max_length": 255}
    )
```

### Add a Foreign Key

```python
def up(schema_editor):
    schema_editor.add_field(
        "Post",
        "author_id",
        "ForeignKeyField",
        {
            "to": "User",
            "related_field": "id",
            "null": True
        }
    )

def down(schema_editor):
    schema_editor.remove_field("Post", "author_id")
```

## Migration Workflow

### Step 1: Modify Your Models

```python
# models.py
class User(Model):
    __tablename__ = "users"
    id = IntegerField(primary_key=True)
    name = TextField(max_length=255)
    email = TextField(max_length=255, unique=True)
    phone = TextField(max_length=20, null=True)  # New field
```

### Step 2: Create Migration

```bash
python manage.py makemigrations
# Creates: migrations/1764179200_add_phone_field.py
```

### Step 3: Review Migration

```python
# Review the generated migration file
# Make adjustments if needed
```

### Step 4: Apply Migration

```bash
python manage.py migrate
# Applies changes to database
```

### Step 5: Commit Changes

```bash
git add migrations/
git commit -m "Add phone field to User model"
```

## Best Practices

1. **Create migrations for every model change** - Keep schema version controlled
2. **Use descriptive names** - `add_user_email_unique` is better than `alter_user`
3. **Keep migrations small** - One logical change per migration
4. **Test migrations** - Run on test database first
5. **Never edit applied migrations** - Create a new migration for changes
6. **Include both up and down** - Ensure migrations are reversible
7. **Use meaningful operation names** - Makes rollback easier to understand

## Handling Migration Conflicts

When multiple developers create migrations:

```bash
# Squash multiple migrations into one
python manage.py squashmigrations 1764179100 1764179200

# Resolve conflicts manually by creating a new migration
python manage.py makemigrations --merge
```

## Data Migrations

Sometimes you need to migrate data, not just schema:

```python
def up(schema_editor):
    # First, update schema
    schema_editor.add_field("User", "full_name", "TextField", {})
    
    # Then migrate data
    db = schema_editor.connection
    cursor = db.cursor()
    cursor.execute("""
        UPDATE users 
        SET full_name = CONCAT(first_name, ' ', last_name)
    """)

def down(schema_editor):
    schema_editor.remove_field("User", "full_name")
```
