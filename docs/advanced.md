# Advanced Features

This guide covers advanced SqlRustler features for power users.

## Window Functions

Window functions allow you to perform calculations across rows while maintaining individual row identity.

### Row Number

```python
from sqlrustler.F import F

# Add sequential numbers
users = User.objects().annotate(
    row_num=F("id").row_number()
).execute()

for user in users:
    print(f"{user.name}: {user._annotations['row_num']}")
```

### Rank and Dense Rank

```python
# Rank with gaps for ties
employees = Employee.objects().annotate(
    salary_rank=F("salary").rank(order_by=["salary"])
).execute()

# Dense rank without gaps for ties
employees = Employee.objects().annotate(
    salary_dense_rank=F("salary").dense_rank(order_by=["salary"])
).execute()
```

### Partitioned Window Functions

```python
# Rank within each department
employees = Employee.objects().annotate(
    dept_rank=F("salary").rank(
        partition_by=["department_id"],
        order_by=["salary"]
    )
).execute()

# Row number within partition
employees = Employee.objects().annotate(
    dept_row_num=F("hire_date").row_number(
        partition_by=["department_id"],
        order_by=["hire_date"]
    )
).execute()
```

## Complex Joins and Relationships

### Select Related (JOIN)

```python
# Load related objects in single query
employees = Employee.objects().select_related(
    "department_id",
    "manager_id"
).execute()

for emp in employees:
    print(f"{emp.name} -> {emp.department_id.name}")
```

### Prefetch Related (Separate Query)

```python
# Load related objects with optimized queries
employees = Employee.objects().prefetch_related(
    "projects",
    "skills"
).execute()

for emp in employees:
    for project in emp.projects:
        print(f"{emp.name} works on {project.name}")
```

## F Expressions

Use `F` objects to reference field values in queries:

```python
from sqlrustler.F import F

# Update with field reference
Employee.objects().update(
    salary=F("salary") * 1.1  # Increase all salaries by 10%
)

# Compare fields
developers = Employee.objects().filter(
    years_experience__gt=F("age") / 2
).execute()
```

## Raw Queries

When you need direct SQL access:

```python
# Raw query with parameters
results = User.objects().raw(
    "SELECT * FROM users WHERE age > ? AND status = ?",
    [18, "active"]
).execute()

# Raw query as dictionaries
results = User.objects().raw(
    "SELECT id, name, email FROM users WHERE id IN ?",
    [[1, 2, 3]]
).execute()
```

## Transactions

Execute multiple operations atomically:

```python
from sqlrustler import DatabaseConnection

# Using context manager (recommended)
with DatabaseConnection.transaction():
    # All operations succeed or all fail
    author = User(name="New Author", email="author@example.com")
    author.save()
    
    post = Post(title="New Post", content="Post content", user_id=author.id)
    post.save()

    # If error occurs here, all changes are rolled back
    if employee.salary < 0:
        raise ValueError("Invalid salary")
```

## Batch Operations

### Bulk Create with Conflict Handling

```python
# Bulk create with unique constraint handling
users = [
    User(name="John", email="john@example.com"),
    User(name="Jane", email="jane@example.com"),
]

# Insert, ignore duplicates
User.objects().bulk_create(users, ignore_conflicts=True)

# Insert, update on conflict
User.objects().bulk_create(
    users,
    update_on_conflict=True,
    update_fields=["name"]
)
```

### Bulk Update

```python
# Update specific users
from datetime import datetime

updates = [
    {"id": 1, "last_login": datetime.now()},
    {"id": 2, "last_login": datetime.now()},
]

User.objects().bulk_update(updates, update_fields=["last_login"])
```

## Advanced Aggregations

### Multiple Aggregations

```python
# Calculate multiple stats at once
stats = Employee.objects().aggregate(
    total_employees=F("id").count(),
    avg_salary=F("salary").avg(),
    total_salary=F("salary").sum(),
    min_salary=F("salary").min(),
    max_salary=F("salary").max(),
    min_years=F("years_experience").min(),
    max_years=F("years_experience").max(),
).first()

for key, value in stats.items():
    print(f"{key}: {value}")
```

### Group By with Multiple Fields

```python
# Group by multiple fields
stats = Employee.objects().group_by("department_id", "role").annotate(
    count=F("id").count(),
    avg_salary=F("salary").avg()
).execute()

for stat in stats:
    print(f"Department: {stat.department_id}, Role: {stat.role}, "
          f"Count: {stat._annotations['count']}")
```

## Connection Management

### Multiple Connections

```python
from sqlrustler import DatabaseConnection

# Main database
main_config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@main-host:5432/main_db",
)
DatabaseConnection.connect(main_config)

# Replica database
replica_config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@replica-host:5432/replica_db",
)
DatabaseConnection.connect(replica_config, alias="replica")

# Use in models
class User(Model):
    __alias__ = "default"  # Uses main database

class UserAnalytics(Model):
    __alias__ = "replica"  # Uses replica database for read-only
```

## Caching and Query Optimization

### Query Result Caching

```python
from functools import lru_cache
from datetime import datetime, timedelta

class UserRepository:
    cache = {}
    cache_time = {}
    CACHE_TTL = timedelta(minutes=5)
    
    @classmethod
    def get_active_users_cached(cls):
        cache_key = "active_users"
        
        # Check cache
        if cache_key in cls.cache:
            cache_age = datetime.now() - cls.cache_time[cache_key]
            if cache_age < cls.CACHE_TTL:
                return cls.cache[cache_key]
        
        # Fetch and cache
        users = User.objects().filter(is_active=True).execute()
        cls.cache[cache_key] = users
        cls.cache_time[cache_key] = datetime.now()
        return users
    
    @classmethod
    def invalidate_cache(cls, key="active_users"):
        if key in cls.cache:
            del cls.cache[key]
            del cls.cache_time[key]
```

## Performance Tuning

### Index Strategies

```python
# Single field index
class User(Model):
    email = TextField(index=True)
    username = TextField(index=True)
```

### Query Optimization Techniques

```python
# 1. Only select needed fields
users = User.objects().values("id", "name").execute()

# 2. Limit results
users = User.objects().limit(100).execute()

# 3. Use select_related for joins
employees = Employee.objects().select_related("department_id").execute()

# 4. Batch operations
User.objects().bulk_create(users)

# 5. Use appropriate filters
recent = User.objects().filter(created_at__gte=cutoff_date).execute()
```

## Error Recovery

### Retry Logic

```python
import time
from functools import wraps

def retry_on_failure(max_attempts=3, delay=1):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_attempts - 1:
                        raise
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

@retry_on_failure(max_attempts=3, delay=1)
def fetch_users():
    return User.objects().execute()
```

### Connection Recovery

```python
from sqlrustler import DatabaseConnection

def ensure_connected():
    """Reconnect if connection is lost"""
    try:
        DatabaseConnection.test_connection()
    except Exception:
        config = DatabaseConfig(...)
        DatabaseConnection.connect(config)
```
