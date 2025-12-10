# Defining Models

Models are the core of SqlRustler. They represent database tables and provide an intuitive interface for data access and manipulation.

## Basic Model Definition

A model is a Python class that inherits from `Model` and represents a database table:

```python
from sqlrustler.model import Model
from sqlrustler.field import IntegerField, TextField, BooleanField

class User(Model):
    __tablename__ = "users"
    __alias__ = "default"
    
    id = IntegerField(primary_key=True)
    name = TextField(max_length=255)
    email = TextField(max_length=255, unique=True)
    is_active = BooleanField(default=True)
```

### Model Attributes

- **`__tablename__`**: The name of the database table (required if not using auto-naming)
- **`__alias__`**: Database connection alias to use (defaults to "default")

### Automatic Table Naming

If you don't specify `__tablename__`, SqlRustler converts the class name to snake_case:

```python
class UserProfile(Model):
    # Automatically maps to "user_profile" table
    id = IntegerField(primary_key=True)
```

## Field Types

SqlRustler provides various field types that map to database columns:

### Text Fields

```python
from sqlrustler.field import TextField, CharField

# Variable-length text (TEXT)
description = TextField()

# Fixed/limited length (VARCHAR)
username = TextField(max_length=50)
```

### Numeric Fields

```python
from sqlrustler.field import IntegerField, FloatField, DecimalField

# Integer (BIGINT/INTEGER)
age = IntegerField()

# Floating point
price = FloatField()

# Precise decimal (for currency, etc.)
balance = DecimalField(max_digits=10, decimal_places=2)
```

### Date/Time Fields

```python
from sqlrustler.field import DateField, DateTimeField

# Date only
birth_date = DateField()

# Date and time with timezone awareness
created_at = DateTimeField()
```

### Boolean Field

```python
from sqlrustler.field import BooleanField

is_verified = BooleanField(default=False)
```

### JSON Field

```python
from sqlrustler.field import JSONField

metadata = JSONField(default={})
```

## Field Options

All fields support the following options:

```python
field = TextField(
    max_length=100,           # Maximum length for text fields
    null=True,                # Allow NULL values
    default="N/A",            # Default value
    unique=True,              # Enforce uniqueness
    primary_key=True,         # Mark as primary key
    index=True                # Create index on this field
)
```

## Relationships

### Foreign Key

Create relationships between tables:

```python
from sqlrustler.field import ForeignKeyField

class Company(Model):
    __tablename__ = "companies"
    id = IntegerField(primary_key=True)
    name = TextField()

class Employee(Model):
    __tablename__ = "employees"
    id = IntegerField(primary_key=True)
    name = TextField()
    company_id = ForeignKeyField(Company, related_field="id")
```

## Model Methods

### Query Interface

Access the query builder through the `objects()` class method:

```python
# Get all records
all_users = User.objects().execute()

# Filter records
active_users = User.objects().filter(is_active=True).execute()

# Get a single record
user = User.objects().filter(id=1).first()

# Count records
user_count = User.objects().count()
```

### Save and Delete

```python
# Create and save
user = User(name="John", email="john@example.com")
user.save()

# Update
user.name = "Jane"
user.save()

# Delete
user.delete()
```

### Bulk Operations

```python
# Bulk create
users = [
    User(name="User1", email="user1@example.com"),
    User(name="User2", email="user2@example.com"),
]
User.objects().bulk_create(users)

# Bulk update
User.objects().filter(is_active=False).update(is_active=True)

# Bulk delete
User.objects().filter(is_active=False).delete()
```

## Example: Complete Model Setup

```python
from sqlrustler.model import Model
from sqlrustler.field import (
    IntegerField, TextField, DateTimeField, 
    BooleanField, ForeignKeyField
)

class Organization(Model):
    __tablename__ = "organizations"
    __alias__ = "default"
    
    id = IntegerField(primary_key=True)
    name = TextField(max_length=255, unique=True)
    created_at = DateTimeField()

class Department(Model):
    __tablename__ = "departments"
    __alias__ = "default"
    
    id = IntegerField(primary_key=True)
    name = TextField(max_length=255)
    organization_id = ForeignKeyField(Organization, related_field="id")

class Employee(Model):
    __tablename__ = "employees"
    __alias__ = "default"
    
    id = IntegerField(primary_key=True)
    name = TextField(max_length=255)
    email = TextField(max_length=255, unique=True)
    department_id = ForeignKeyField(Department, related_field="id")
    is_active = BooleanField(default=True)
    created_at = DateTimeField()
```
