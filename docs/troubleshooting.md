# Troubleshooting Guide

Common issues and their solutions when using SqlRustler.

## Connection Issues

### Cannot Connect to Database

**Error**: `ConnectionRefused` or `Connection timeout`

**Solutions**:
1. Verify database server is running:
   ```bash
   # PostgreSQL
   pg_isready -h localhost -p 5432
   
   # MySQL
   mysqladmin ping -u root -p
   ```

2. Check connection URL:
   ```python
   # Ensure format is correct
   config = DatabaseConfig(
       driver=DatabaseType.Postgres,
       url="postgresql://user:password@localhost:5432/database",
   )
   ```

3. Verify credentials:
   ```bash
   # Test with command line
   psql -U user -h localhost -d database
   mysql -u user -h localhost -p database
   ```

4. Check firewall/network:
   ```bash
   # Test connection
   telnet localhost 5432
   ```

### Authentication Failed

**Error**: `Authentication failed` or `Invalid password`

**Solutions**:
1. Verify username and password in connection URL
2. Ensure user exists in database:
   ```sql
   -- PostgreSQL
   SELECT * FROM pg_user WHERE usename = 'myuser';
   
   -- MySQL
   SELECT user, host FROM mysql.user;
   ```

3. Check user permissions:
   ```sql
   -- PostgreSQL
   GRANT ALL PRIVILEGES ON DATABASE mydatabase TO myuser;
   
   -- MySQL
   GRANT ALL PRIVILEGES ON mydatabase.* TO 'myuser'@'localhost';
   ```

### Too Many Connections

**Error**: `Too many connections` or `Connection pool exhausted`

**Solutions**:
1. Reduce `max_connections`:
   ```python
   config = DatabaseConfig(
       driver=DatabaseType.Postgres,
       url="postgresql://user:password@localhost:5432/db",
       max_connections=10,  # Reduce from 20
   )
   ```

2. Add `idle_timeout` to close unused connections:
   ```python
   config = DatabaseConfig(
       driver=DatabaseType.Postgres,
       url="postgresql://user:password@localhost:5432/db",
       idle_timeout=60,  # Close idle connections after 60 seconds
   )
   ```

3. Check for connection leaks:
   ```python
   # Ensure connections are properly closed
   DatabaseConnection.disconnect()
   ```

## Query Issues

### No Results Returned

**Problem**: Query executes but returns empty results

**Solutions**:
1. Verify filter conditions:
   ```python
   # Debug filter
   users = User.objects().filter(name="John")
   print(users)  # Check SQL being generated
   
   # Try without filter
   all_users = User.objects().execute()
   print(f"Total users: {len(all_users)}")
   ```

2. Check field names:
   ```python
   # Ensure field names match database columns
   # NOT tablename.field, just field
   users = User.objects().filter(name="John")  # ✓ Correct
   users = User.objects().filter(users.name="John")  # ✗ Wrong
   ```

3. Verify data exists in database:
   ```sql
   -- Direct database query
   SELECT * FROM users WHERE name = 'John';
   ```

### Query Returns Wrong Type

**Error**: Results are strings instead of integers, or vice versa

**Solutions**:
1. Check field type definition:
   ```python
   class User(Model):
       __tablename__ = "users"
       age = IntegerField()  # Should be IntegerField, not TextField
   ```

2. Verify database schema:
   ```sql
   -- PostgreSQL
   \d users;
   
   -- MySQL
   DESCRIBE users;
   ```

3. Force type conversion:
   ```python
   users = User.objects().execute()
   for user in users:
       age = int(user.age)  # Convert if needed
   ```

### Null/None Values in Results

**Problem**: Unexpected NULL values in results

**Solutions**:
1. Check field definition:
   ```python
   # Allow NULL
   bio = TextField(null=True)
   
   # Disallow NULL (default)
   name = TextField(null=False)
   ```

2. Set default values:
   ```python
   class User(Model):
       status = TextField(default="active")
       is_verified = BooleanField(default=False)
   ```

3. Handle NULL in queries:
   ```python
   # Get records without NULL
   users = User.objects().filter(bio__isnull=False).execute()
   
   # Get records with NULL
   users = User.objects().filter(bio__isnull=True).execute()
   ```

### Performance Issues

**Problem**: Queries are slow

**Solutions**:
1. Identify N+1 queries:
   ```python
   # ❌ N+1 Problem
   employees = Employee.objects().execute()
   for emp in employees:
       dept = emp.department_id  # Separate query per employee
   
   # ✅ Use select_related
   employees = Employee.objects().select_related("department_id").execute()
   ```

2. Use database indexes:
   ```python
   class User(Model):
       email = TextField(index=True)  # Add index for frequently filtered fields
   ```

3. Limit result set:
   ```python
   # Get only needed columns
   users = User.objects().values("id", "name").execute()
   
   # Limit number of results
   users = User.objects().limit(100).execute()
   ```

4. Add query timeout:
   ```python
   # Run expensive query with timeout
   try:
       results = User.objects().raw(
           "SELECT * FROM large_table WHERE condition",
           timeout=30
       ).execute()
   except TimeoutError:
       print("Query took too long")
   ```

## Model Issues

### Model Not Found / Import Error

**Error**: `ModuleNotFoundError` or `ImportError`

**Solutions**:
1. Verify model file exists:
   ```bash
   ls -la models.py
   ```

2. Check Python path:
   ```python
   import sys
   print(sys.path)
   ```

3. Ensure models.py is properly imported:
   ```python
   # In your application startup
   from models import User, Post  # Import models
   from sqlrustler.sqlrustler import DatabaseConnection
   
   config = DatabaseConfig(...)
   DatabaseConnection.connect(config)
   ```

### Foreign Key Errors

**Error**: `ForeignKeyError` or related record not found

**Solutions**:
1. Ensure related record exists:
   ```python
   # Create related record first
   author = User(name="Acme Author", email="author@acme.com")
   author.save()
   
   # Then create child record
   post = Post(title="Acme Post", content="Post content", user_id=author.id)
   post.save()
   ```

2. Check field type:
   ```python
   # ForeignKeyField should reference the related model
   class Post(Model):
       user_id = ForeignKeyField(User, related_field="id")
   ```

3. Verify `related_field` exists:
   ```python
   # Ensure User has id field
   class User(Model):
       id = IntegerField(primary_key=True)
       name = TextField()
       email = TextField()
   ```

## Migration Issues

### Migration Fails to Apply

**Error**: `Migration failed` or schema mismatch

**Solutions**:
1. Check migration file syntax:
   ```python
   # Ensure proper function definitions
   def up(schema_editor):
       schema_editor.add_field(...)
   
   def down(schema_editor):
       schema_editor.remove_field(...)
   ```

2. Check for conflicts:
   ```bash
   python manage.py showmigrations
   ```

3. Manually review migration:
   ```python
   # Check migration file for errors
   cat migrations/1764179025_initial.py
   ```

### Rollback Problems

**Problem**: Cannot rollback migration

**Solutions**:
1. List applied migrations:
   ```bash
   python manage.py showmigrations
   ```

2. Check migration dependencies:
   ```python
   # Ensure down() method is reversible
   def down(schema_editor):
       # Undo what up() did
       schema_editor.remove_field("User", "bio")
   ```

3. Create recovery migration:
   ```bash
   python manage.py makemigrations --empty --name recovery
   ```

## Data Issues

### Duplicate Records

**Problem**: Duplicate data in database

**Solutions**:
1. Add unique constraint:
   ```python
   class User(Model):
       email = TextField(unique=True)
   ```

2. Check for duplicates:
   ```sql
   SELECT email, COUNT(*) FROM users GROUP BY email HAVING COUNT(*) > 1;
   ```

3. Remove duplicates:
   ```sql
   -- PostgreSQL
   DELETE FROM users a USING users b 
   WHERE a.id < b.id AND a.email = b.email;
   ```

### Data Type Mismatch

**Error**: `TypeError` or `ValueError` when saving

**Solutions**:
1. Verify field types:
   ```python
   # Ensure data matches field type
   user = User()
   user.age = "25"  # ❌ Wrong - should be int
   user.age = 25    # ✓ Correct
   ```

2. Add type conversion:
   ```python
   user.age = int(age_value)
   user.email = str(email_value).lower()
   ```

3. Use validators:
   ```python
   from sqlrustler.field import IntegerField
   
   class User(Model):
       age = IntegerField(validators=[validate_age])
   ```

## Debugging

### Enable Query Logging

```python
import logging

# Enable SQL query logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("sqlrustler")
logger.setLevel(logging.DEBUG)

# View SQL being generated
users = User.objects().filter(name="John")
print(users.query)  # Print the SQL
```

### Print Query

```python
# Get SQL without executing
users_query = User.objects().filter(name="John")
print(f"SQL: {users_query.to_sql()}")

# Count affected rows
count = User.objects().filter(name="John").count()
print(f"Matched {count} records")
```

### Inspect Results

```python
# Check result type and structure
users = User.objects().execute()
print(f"Type: {type(users)}")
print(f"Count: {len(users)}")

if users:
    user = users[0]
    print(f"First user: {user}")
    print(f"Fields: {user._fields}")
    print(f"Values: {vars(user)}")
```

## Getting Help

- Check the [API Reference](./api.md)
- Review [Common Patterns](./patterns.md)
- Check project [Issues on GitHub](https://github.com/DVNghiem/SqlRustler/issues)
- See [Contributing](./contributing.md) for bug reports
