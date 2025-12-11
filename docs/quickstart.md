# Quick Start

## Define Models

Create a `models.py` file to define your database tables:

```python
from sqlrustler.model import Model
from sqlrustler.field import IntegerField, TextField, ForeignKeyField

class User(Model):
    __tablename__ = "users"
    __alias__ = "default"
    id = IntegerField(primary_key=True)
    name = TextField()
    email = TextField()

class Post(Model):
    __tablename__ = "posts"
    __alias__ = "default"
    id = IntegerField(primary_key=True)
    title = TextField()
    content = TextField()
    user_id = ForeignKeyField(User, related_field="id")
```

## Connect to the Database

Configure and connect to your database:

```python
from sqlrustler import DatabaseConfig, DatabaseType, DatabaseConnection

config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@localhost:5432/stag_v2",
    max_connections=10,
    min_connections=1,
    idle_timeout=30,
)

DatabaseConnection.connect(config)
```

## Query the Database

Perform queries using the ORM:

```python
from sqlrustler import F

# Fetch all users with row numbers
users = User.objects().annotate(row_num=F("id").row_number()).execute()
for user in users:
    print(f"User: {user.name}, Row Number: {user._annotations['row_num']}")

# Filter and select related data
posts = Post.objects().filter(title__contains="python").select_related("user_id").execute()
for post in posts:
    print(f"Post: {post.title}, Author: {post.user_id.name}")

# Aggregate data
result = User.objects().aggregate(count=F("id").count())
print(f"Total users: {result['count']}")

# Raw results for custom queries
results = User.objects().select("name", "email").raw().execute()
print(results)  # [{'name': 'John Doe', 'email': 'john@example.com'}, ...]
```