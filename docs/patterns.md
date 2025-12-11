# Common Patterns and Best Practices

This guide covers common usage patterns and best practices for SqlRustler.

## Query Patterns

### The Repository Pattern

Encapsulate query logic in reusable methods:

```python
from sqlrustler.model import Model
from sqlrustler.field import IntegerField, TextField, BooleanField

class User(Model):
    __tablename__ = "users"
    id = IntegerField(primary_key=True)
    name = TextField()
    email = TextField()
    is_active = BooleanField()
    
    @classmethod
    def get_active_users(cls):
        """Get all active users"""
        return cls.objects().filter(is_active=True)
    
    @classmethod
    def find_by_email(cls, email):
        """Find user by email"""
        return cls.objects().filter(email=email).first()
    
    @classmethod
    def search_by_name(cls, name):
        """Search users by name (case-insensitive)"""
        return cls.objects().filter(name__icontains=name)

# Usage
active_users = User.get_active_users().execute()
user = User.find_by_email("john@example.com")
search_results = User.search_by_name("john").execute()
```

### Bulk Operations for Performance

When working with large datasets, use bulk operations:

```python
# ❌ Slow - N+1 queries
for user_data in large_list:
    user = User(name=user_data['name'], email=user_data['email'])
    user.save()

# ✅ Fast - Single query
users = [
    User(name=data['name'], email=data['email'])
    for data in large_list
]
User.objects().bulk_create(users)

# ✅ Update multiple records
User.objects().filter(is_active=False).update(is_active=True)

# ✅ Delete multiple records
User.objects().filter(created_at__lt=cutoff_date).delete()
```

### Efficient Relationship Loading

Avoid N+1 query problems:

```python
# ❌ N+1 Problem - Separate query for each related object
employees = Employee.objects().execute()
for emp in employees:
    dept = emp.department_id  # Triggers separate query

# ✅ Use select_related - Single join
employees = Employee.objects().select_related("department_id").execute()
for emp in employees:
    dept = emp.department_id  # Already loaded

# ✅ Use prefetch_related - Separate optimized query
employees = Employee.objects().prefetch_related("projects").execute()
```

## Filtering Patterns

### Safe NULL Handling

```python
# Get records with phone number
users_with_phone = User.objects().filter(phone__isnull=False).execute()

# Get records without phone number
users_without_phone = User.objects().filter(phone__isnull=True).execute()
```

### Range Queries

```python
from datetime import datetime, timedelta

# Get records within a date range
week_ago = datetime.now() - timedelta(days=7)
recent_users = User.objects().filter(
    created_at__gte=week_ago,
    created_at__lt=datetime.now()
).execute()

# Get records in a numeric range
adults = User.objects().filter(age__gte=18, age__lt=65).execute()
```

### Combining Multiple Filters

```python
# AND condition (default)
active_adults = User.objects().filter(
    is_active=True,
    age__gte=18
).execute()

# OR condition with Q objects
from sqlrustler.Q import Q

premium_users = User.objects().filter(
    Q(is_premium=True) | Q(is_vip=True)
).execute()

# Complex combinations
results = User.objects().filter(
    Q(is_active=True) & (
        Q(subscription_type="premium") |
        Q(is_founder=True)
    )
).execute()
```

## Data Transformation Patterns

### Mapping Results

```python
# Transform query results
users = User.objects().values("name", "email").execute()
user_dict = {user['email']: user['name'] for user in users}

# Extract single column
names = User.objects().values_list("name", flat=True).execute()

# Create custom objects
users = User.objects().execute()
user_emails = [user.email for user in users]
```

### Pagination

```python
def paginate(model_class, page=1, page_size=20):
    """Generic pagination helper"""
    offset = (page - 1) * page_size
    return model_class.objects().offset(offset).limit(page_size).execute()

# Usage
page_1 = paginate(User, page=1, page_size=20)
page_2 = paginate(User, page=2, page_size=20)
```

### Sorting and Ordering

```python
# Multiple sort criteria
results = User.objects().order_by("department", "-salary").execute()

# Dynamic sorting
sort_field = request.get("sort", "name")
reverse = request.get("reverse", False) == "true"
sort_key = f"-{sort_field}" if reverse else sort_field
users = User.objects().order_by(sort_key).execute()
```

## Aggregation Patterns

### Running Totals

```python
from sqlrustler.F import F

# Add row numbers
employees = Employee.objects().annotate(
    row_num=F("id").row_number(order_by=["salary"])
).execute()

# Running count within department
employees = Employee.objects().annotate(
    dept_count=F("id").row_number(
        partition_by=["department_id"],
        order_by=["hire_date"]
    )
).execute()
```

### Summary Statistics

```python
stats = User.objects().aggregate(
    total=F("id").count(),
    avg_age=F("age").avg(),
    min_age=F("age").min(),
    max_age=F("age").max(),
).first()

print(f"Total users: {stats['total']}")
print(f"Average age: {stats['avg_age']}")
```

### Group By with Aggregates

```python
# Group by department and count
dept_stats = Employee.objects().group_by("department_id").annotate(
    employee_count=F("id").count(),
    avg_salary=F("salary").avg(),
    total_salary=F("salary").sum()
).execute()

for stat in dept_stats:
    print(f"Department {stat.department_id}: "
          f"{stat._annotations['employee_count']} employees, "
          f"avg salary: {stat._annotations['avg_salary']}")
```

## Error Handling Patterns

### Graceful Error Handling

```python
from sqlrustler.exceptions import DoesNotExist

# Safe single record lookup
try:
    user = User.objects().filter(id=user_id).get()
except DoesNotExist:
    print("User not found")
    user = None

# Safe creation or update
user, created = User.objects().get_or_create(
    email="user@example.com",
    defaults={"name": "New User"}
)

if created:
    print("User created")
else:
    print("User already exists")
```

### Validation Patterns

```python
# Validate before save
class User(Model):
    __tablename__ = "users"
    id = IntegerField(primary_key=True)
    name = TextField(max_length=255)
    email = TextField(max_length=255)
    age = IntegerField(null=True)
    
    def validate(self):
        """Validate user data"""
        if not self.email:
            raise ValueError("Email is required")
        if "@" not in self.email:
            raise ValueError("Invalid email format")
        if self.age and self.age < 0:
            raise ValueError("Age cannot be negative")
    
    def save(self):
        self.validate()
        super().save()

# Usage
try:
    user = User(name="John", email="invalid-email", age=25)
    user.save()
except ValueError as e:
    print(f"Validation error: {e}")
```

## Transaction Patterns

### Transactional Operations

```python
from sqlrustler import DatabaseConnection

# Wrap operations in transaction
with DatabaseConnection.transaction():
    # Create related objects
    author = User(name="Acme Author", email="author@acme.com")
    author.save()
    
    post = Post(title="Acme Post", content="Post content", user_id=author.id)
    post.save()
    
    # If any operation fails, all are rolled back
```

## Caching Patterns

### Simple Query Caching

```python
from functools import lru_cache
from datetime import datetime, timedelta

class UserCache:
    _cache = {}
    _cache_time = {}
    
    @classmethod
    def get_user(cls, user_id):
        """Get user with caching"""
        if user_id in cls._cache:
            cache_age = datetime.now() - cls._cache_time[user_id]
            if cache_age < timedelta(minutes=5):
                return cls._cache[user_id]
        
        user = User.objects().filter(id=user_id).first()
        cls._cache[user_id] = user
        cls._cache_time[user_id] = datetime.now()
        return user
    
    @classmethod
    def clear_cache(cls):
        """Clear all cached data"""
        cls._cache.clear()
        cls._cache_time.clear()
```

## Testing Patterns

### Model Testing

```python
import pytest

class TestUser:
    def test_create_user(self):
        """Test user creation"""
        user = User(name="John", email="john@example.com")
        user.save()
        
        retrieved = User.objects().filter(id=user.id).first()
        assert retrieved.name == "John"
        assert retrieved.email == "john@example.com"
    
    def test_update_user(self):
        """Test user update"""
        user = User(name="John", email="john@example.com")
        user.save()
        
        user.name = "Jane"
        user.save()
        
        retrieved = User.objects().filter(id=user.id).first()
        assert retrieved.name == "Jane"
    
    def test_filter_users(self):
        """Test filtering"""
        User(name="John", email="john@example.com").save()
        User(name="Jane", email="jane@example.com").save()
        
        results = User.objects().filter(name__contains="John").execute()
        assert len(results) == 1
        assert results[0].name == "John"
```

## Performance Tips

1. **Use `select_related()` for foreign keys** to reduce queries
2. **Use `values()` or `values_list()`** when you don't need model instances
3. **Batch operations** with `bulk_create()` and `bulk_update()`
4. **Add database indexes** on frequently filtered fields
5. **Use `.only()` and `.defer()`** to load specific fields
6. **Consider denormalization** for complex aggregations
7. **Monitor query performance** with query logging
