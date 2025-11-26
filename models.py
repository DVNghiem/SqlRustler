from sqlrustler import Model, IntegerField, CharField

class User(Model):
    id = IntegerField(primary_key=True)
    username = CharField(max_length=50, unique=True)
    email = CharField(max_length=100)

class Post(Model):
    id = IntegerField(primary_key=True)
    title = CharField(max_length=200)
    content = CharField(max_length=1000)
