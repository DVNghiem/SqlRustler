# Quick Reference

A quick lookup guide for common SqlRustler operations.

## Setup

```python
from sqlrustler.sqlrustler import DatabaseConfig, DatabaseType, DatabaseConnection
from sqlrustler.model import Model
from sqlrustler.field import IntegerField, TextField, ForeignKeyField

# Configure database
config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@localhost:5432/db"
)
DatabaseConnection.connect(config)

# Define model
class User(Model):
    __tablename__ = "users"
    id = IntegerField(primary_key=True)
    name = TextField()
```

## Queries

### Fetch Data

```python
# Get all
users = User.objects().execute()

# Get one
user = User.objects().filter(id=1).first()

# Filter
users = User.objects().filter(name="John").execute()
users = User.objects().filter(age__gte=18).execute()

# Multiple filters
users = User.objects().filter(is_active=True, age__gte=18).execute()

# OR conditions
from sqlrustler.Q import Q
users = User.objects().filter(Q(name="John") | Q(name="Jane")).execute()

# Order
users = User.objects().order_by("name").execute()
users = User.objects().order_by("-created_at").execute()

# Limit
users = User.objects().limit(10).execute()
users = User.objects().offset(10).limit(10).execute()

# Distinct
distinct = User.objects().distinct("department").execute()

# Count
count = User.objects().count()

# Exists
exists = User.objects().filter(id=1).exists()
```

### Select Columns

```python
# Specific columns as model instances
users = User.objects().select("id", "name").execute()

# As dictionaries
users = User.objects().values("id", "name").execute()

# As list
names = User.objects().values_list("name", flat=True).execute()

# Multiple columns as tuples
pairs = User.objects().values_list("id", "name").execute()
```

### Relationships

```python
# Join (single query)
employees = Employee.objects().select_related("department_id").execute()

# Related data (separate queries)
employees = Employee.objects().prefetch_related("projects").execute()
```

## Create/Update/Delete

```python
# Create
user = User(name="John", email="john@example.com")
user.save()

# Update
user.name = "Jane"
user.save()

# Delete
user.delete()

# Bulk create
users = [User(name=f"User{i}") for i in range(100)]
User.objects().bulk_create(users)

# Bulk update
User.objects().filter(is_active=False).update(is_active=True)

# Bulk delete
User.objects().filter(created_at__lt=cutoff).delete()
```

## Aggregations

```python
from sqlrustler.F import F

# Count
total = User.objects().count()

# Sum
total = User.objects().aggregate(
    total=F("salary").sum()
).first()

# Average
avg = User.objects().aggregate(
    avg=F("salary").avg()
).first()

# Min/Max
stats = User.objects().aggregate(
    min_age=F("age").min(),
    max_age=F("age").max()
).first()

# Annotate
employees = Employee.objects().annotate(
    rank=F("salary").rank(order_by=["salary"])
).execute()
```

## Lookups

```python
# Exact (default)
User.objects().filter(name="John").execute()

# Contains
User.objects().filter(email__contains="gmail").execute()

# Starts with
User.objects().filter(name__startswith="Jo").execute()

# Ends with
User.objects().filter(email__endswith="@gmail.com").execute()

# Greater than
User.objects().filter(age__gt=18).execute()

# Greater than or equal
User.objects().filter(age__gte=18).execute()

# Less than
User.objects().filter(age__lt=65).execute()

# Less than or equal
User.objects().filter(age__lte=65).execute()

# In list
User.objects().filter(id__in=[1, 2, 3]).execute()

# Is NULL
User.objects().filter(phone__isnull=True).execute()

# Is not NULL
User.objects().filter(phone__isnull=False).execute()
```

## Transactions

```python
from sqlrustler.sqlrustler import DatabaseConnection

with DatabaseConnection.transaction():
    user = User(name="John")
    user.save()
    # Rollback if error occurs
```

## Expressions

```python
from sqlrustler.F import F

# Update with expression
Employee.objects().update(
    salary=F("salary") * 1.1
)

# Filter with expression
employees = Employee.objects().filter(
    years_exp__gt=F("age") / 2
).execute()
```

## Connection

```python
# Connect
config = DatabaseConfig(...)
DatabaseConnection.connect(config)

# Test connection
DatabaseConnection.test_connection()

# Disconnect
DatabaseConnection.disconnect()

# Multiple connections
DatabaseConnection.connect(config, alias="analytics")

class AnalyticsEvent(Model):
    __alias__ = "analytics"
```

## Common Patterns

### Get or Create

```python
user = User.objects().filter(email="john@example.com").first()
if not user:
    user = User(name="John", email="john@example.com")
    user.save()
```

### Pagination

```python
page = 2
page_size = 20
offset = (page - 1) * page_size

users = User.objects().offset(offset).limit(page_size).execute()
```

### Filter Chain

```python
query = User.objects()

if search_name:
    query = query.filter(name__contains=search_name)

if min_age:
    query = query.filter(age__gte=min_age)

if active_only:
    query = query.filter(is_active=True)

results = query.execute()
```

### Safe Access

```python
try:
    user = User.objects().filter(id=1).get()
except User.DoesNotExist:
    user = None
```

### Batch Processing

```python
# Process in chunks
batch_size = 1000
offset = 0

while True:
    users = User.objects().offset(offset).limit(batch_size).execute()
    if not users:
        break
    
    for user in users:
        process(user)
    
    offset += batch_size
```

## Field Types

```python
from sqlrustler.field import (
    IntegerField,
    TextField,
    FloatField,
    DecimalField,
    DateField,
    DateTimeField,
    BooleanField,
    JSONField,
    ForeignKeyField
)

# Integer
age = IntegerField()

# String
name = TextField(max_length=255)

# Floating point
price = FloatField()

# Decimal (precise)
balance = DecimalField(max_digits=10, decimal_places=2)

# Date
birth_date = DateField()

# DateTime
created_at = DateTimeField()

# Boolean
is_active = BooleanField(default=True)

# JSON
metadata = JSONField(default={})

# Foreign key
user_id = ForeignKeyField(User, related_field="id")
```

## Field Options

```python
field = TextField(
    max_length=100,      # Max length
    null=True,           # Allow NULL
    default="N/A",       # Default value
    unique=True,         # Must be unique
    primary_key=True,    # Primary key
    index=True           # Create index
)
```

## Common Errors & Solutions

| Error | Solution |
|-------|----------|
| `ConnectionRefused` | Check database is running and credentials are correct |
| `No results returned` | Verify filter conditions match data in database |
| `DoesNotExist` | Use `.first()` instead of `.get()` to avoid exceptions |
| `Too many connections` | Reduce `max_connections` or add `idle_timeout` |
| `N+1 queries` | Use `select_related()` for foreign keys |

## Performance Tips

1. Use `select_related()` for foreign keys
2. Use `values()` when you don't need model instances
3. Use `bulk_create()` for inserting many records
4. Add indexes to frequently filtered fields
5. Use `.limit()` to restrict large result sets
6. Use `.count()` instead of `len(queryset.execute())`

## Resources

- [Full Documentation](index.md)
- [Database Setup Guide](database-setup.md)
- [Defining Models](models.md)
- [Querying Data](querying.md)
- [Common Patterns](patterns.md)
- [Troubleshooting](troubleshooting.md)
