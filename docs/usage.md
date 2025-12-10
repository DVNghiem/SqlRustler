# Usage Examples

## Filtering and Ordering

```python
# Filter users by email and order by name
users = User.objects().filter(email__endswith="@example.com").order_by("name").execute()
```

## Bulk Create

```python
author = User(name="John Doe", email="john@example.com")
author.save()
posts = [
    Post(title=f"Post {i}", content=f"Content for post {i}", user_id=author)
    for i in range(3)
]
Post.objects().bulk_create(posts)
```

## Custom Select and Values

```python
# Select specific fields as dictionaries
results = User.objects().values("name", "email").execute()
# [{'name': 'John Doe', 'email': 'john@example.com'}, ...]

# Flat values list
names = User.objects().values_list("name", flat=True).execute()
# ['John Doe', 'Jane Smith', ...]
```

## Window Functions

```python
# Rank posts by user
posts = Post.objects().annotate(
    rank=F("id").rank(partition_by=["user_id"])
).execute()
for post in posts:
    print(f"Post: {post.title}, Rank: {post._annotations['rank']}")
```