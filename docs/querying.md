# Querying Data

SqlRustler provides a powerful, chainable query builder API for constructing and executing database queries.

## Query Basics

All queries start with the `objects()` class method on a Model:

```python
from models import User

# Get query builder
query = User.objects()

# Execute the query
users = query.execute()
```

## Filtering

### Simple Filters

Filter records using field lookups:

```python
# Exact match
active_users = User.objects().filter(is_active=True).execute()

# Multiple conditions (AND)
users = User.objects().filter(is_active=True, age=25).execute()
```

### Lookup Operators

Use lookup operators with double underscore syntax:

```python
# String lookups
User.objects().filter(name__contains="John").execute()          # LIKE '%John%'
User.objects().filter(name__startswith="Jo").execute()          # LIKE 'Jo%'
User.objects().filter(name__endswith="hn").execute()             # LIKE '%hn'
User.objects().filter(email__icontains="gmail").execute()        # Case-insensitive

# Numeric comparisons
User.objects().filter(age__gt=18).execute()                      # Greater than
User.objects().filter(age__gte=18).execute()                     # Greater than or equal
User.objects().filter(age__lt=65).execute()                      # Less than
User.objects().filter(age__lte=65).execute()                     # Less than or equal
User.objects().filter(age__exact=25).execute()                   # Exact match

# IN operator
User.objects().filter(id__in=[1, 2, 3]).execute()

# Is NULL
User.objects().filter(phone__isnull=True).execute()
```

### Complex Filters with Q Objects

Combine multiple conditions with AND/OR logic:

```python
from sqlrustler.Q import Q

# OR condition
users = User.objects().filter(
    Q(name="John") | Q(name="Jane")
).execute()

# AND with nested OR
users = User.objects().filter(
    Q(is_active=True) & (Q(age__gte=18) | Q(is_employee=True))
).execute()

# NOT condition
inactive_users = User.objects().filter(~Q(is_active=True)).execute()
```

## Selecting and Ordering

### Select Specific Columns

```python
# Select all fields (default)
users = User.objects().execute()

# Select specific fields
users = User.objects().select("name", "email").execute()

# Select related fields (join)
employees = Employee.objects().select_related("department_id").execute()
for emp in employees:
    print(f"{emp.name} works in {emp.department_id.name}")

# Prefetch related (separate query)
employees = Employee.objects().prefetch_related("projects").execute()
```

### Order Results

```python
# Order ascending
users = User.objects().order_by("name").execute()

# Order descending
users = User.objects().order_by("-created_at").execute()

# Multiple order columns
users = User.objects().order_by("department", "-salary").execute()
```

## Limiting and Pagination

```python
# Limit results
top_10 = User.objects().limit(10).execute()

# Offset (pagination)
page_2 = User.objects().offset(10).limit(10).execute()

# Slice notation
users = User.objects()[10:20]  # Offset 10, limit 10
```

## Aggregations and Annotations

### Aggregation Functions

```python
from sqlrustler.F import F

# Count
total_users = User.objects().count()

# Sum
total_salary = User.objects().aggregate(
    total=F("salary").sum()
).execute()

# Average
avg_salary = User.objects().aggregate(
    average=F("salary").avg()
).execute()

# Min/Max
min_age = User.objects().aggregate(
    min_age=F("age").min()
).execute()

max_age = User.objects().aggregate(
    max_age=F("age").max()
).execute()
```

### Annotations (Adding Computed Fields)

```python
# Add a window function
employees = Employee.objects().annotate(
    row_num=F("id").row_number()
).execute()

for emp in employees:
    print(f"{emp.name}: {emp._annotations['row_num']}")

# Rank within partition
employees = Employee.objects().annotate(
    dept_rank=F("salary").rank(partition_by=["department_id"])
).execute()

# Dense rank
employees = Employee.objects().annotate(
    dense_rank=F("salary").dense_rank(order_by=["salary"])
).execute()
```

## Values and Lists

### Get Dictionaries

```python
# Get records as dictionaries
users = User.objects().values("name", "email").execute()
# [{'name': 'John', 'email': 'john@example.com'}, ...]

# Get all fields as dictionary
users = User.objects().values().execute()
```

### Get Flat Lists

```python
# Get single column as list
names = User.objects().values_list("name", flat=True).execute()
# ['John', 'Jane', 'Bob']

# Get multiple columns as tuples
users = User.objects().values_list("name", "email").execute()
# [('John', 'john@example.com'), ('Jane', 'jane@example.com')]
```

### Distinct Values

```python
# Get distinct values
departments = User.objects().distinct("department").execute()
```

## Grouping

```python
# Group by department and count
from sqlrustler.F import F

stats = User.objects().group_by("department_id").annotate(
    count=F("id").count()
).execute()
```

## Combining Query Methods

Queries are chainable - methods can be combined in any order:

```python
users = (User.objects()
    .filter(is_active=True)
    .filter(age__gte=18)
    .order_by("-created_at")
    .select("name", "email")
    .limit(20)
    .execute()
)
```

## Single Record Queries

```python
# Get first record (or None)
user = User.objects().filter(id=1).first()

# Get or raise exception
try:
    user = User.objects().filter(id=999).get()
except User.DoesNotExist:
    print("User not found")

# Exists check
exists = User.objects().filter(email="john@example.com").exists()
```

## Raw Queries

When you need more control, use raw SQL:

```python
# Execute raw SQL
results = User.objects().raw(
    "SELECT * FROM users WHERE age > ? AND is_active = ?",
    [18, True]
).execute()
```

## Performance Tips

1. **Use `select_related()` for foreign keys** - Reduces database queries
2. **Use `prefetch_related()` for reverse relations** - More efficient than N+1 queries
3. **Use `.values()` or `.values_list()`** - Faster when you don't need model instances
4. **Use `.only()`** - Load specific fields only
5. **Use `bulk_create()` for multiple inserts** - Much faster than individual saves
