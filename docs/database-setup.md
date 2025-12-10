# Database Setup and Configuration

This guide covers setting up database connections and configuration in SqlRustler.

## Database Connection

### Basic Setup

```python
from sqlrustler.sqlrustler import DatabaseConfig, DatabaseType, DatabaseConnection

# PostgreSQL
config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@localhost:5432/mydatabase",
    max_connections=10,
    min_connections=1,
    idle_timeout=30,
)

DatabaseConnection.connect(config)
```

### Supported Databases

SqlRustler supports multiple database systems:

```python
# PostgreSQL
config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@localhost:5432/db",
)

# MySQL
config = DatabaseConfig(
    driver=DatabaseType.MySQL,
    url="mysql://user:password@localhost:3306/db",
)

# SQLite
config = DatabaseConfig(
    driver=DatabaseType.SQLite,
    url="sqlite:///path/to/database.db",
)
```

## Configuration Parameters

### Connection Settings

```python
config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@host:5432/database",
    
    # Connection pool settings
    max_connections=20,           # Maximum pool size
    min_connections=5,            # Minimum pool size
    idle_timeout=300,             # Idle connection timeout (seconds)
    
    # Retry settings
    max_retries=3,                # Maximum retry attempts
    retry_delay=1,                # Delay between retries (seconds)
    
    # SSL settings
    ssl_mode="require",           # SSL mode (disable, allow, prefer, require)
)
```

## Multiple Database Connections

You can configure multiple database aliases:

```python
from sqlrustler.sqlrustler import DatabaseConnection

# Primary database
primary_config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@primary:5432/main_db",
)
DatabaseConnection.connect(primary_config)

# Analytics database
analytics_config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@analytics:5432/analytics_db",
)
DatabaseConnection.connect(analytics_config, alias="analytics")

# Use in models
class User(Model):
    __alias__ = "default"  # Uses primary connection
    
class AnalyticsEvent(Model):
    __alias__ = "analytics"  # Uses analytics connection
```

## Environment Variables

Store sensitive configuration in environment variables:

```python
import os
from sqlrustler.sqlrustler import DatabaseConfig, DatabaseType, DatabaseConnection

config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url=os.getenv("DATABASE_URL"),
    max_connections=int(os.getenv("DB_MAX_CONNECTIONS", 10)),
    min_connections=int(os.getenv("DB_MIN_CONNECTIONS", 1)),
)

DatabaseConnection.connect(config)
```

### .env File Example

```
DATABASE_URL=postgresql://user:password@localhost:5432/mydatabase
DB_MAX_CONNECTIONS=20
DB_MIN_CONNECTIONS=5
DB_IDLE_TIMEOUT=300
```

## Connection Testing

### Verify Connection

```python
try:
    # Test connection
    DatabaseConnection.test_connection()
    print("✓ Database connection successful")
except Exception as e:
    print(f"✗ Connection failed: {e}")
```

### Health Check

```python
from sqlrustler.sqlrustler import DatabaseConnection

# Perform health check
is_healthy = DatabaseConnection.is_healthy()
if is_healthy:
    print("Database is healthy")
else:
    print("Database connection issues detected")
```

## Connection Pooling

SqlRustler uses connection pooling for optimal performance:

```python
config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@localhost:5432/db",
    max_connections=20,        # Max simultaneous connections
    min_connections=5,         # Keep minimum connections alive
    idle_timeout=300,          # Close idle connections after 5 min
)
```

### Connection Pool Tuning

- **High concurrency**: Increase `max_connections` (e.g., 50-100)
- **Low concurrency**: Keep `max_connections` moderate (e.g., 5-10)
- **Long-running queries**: Increase `idle_timeout`
- **Short connections**: Decrease `idle_timeout` to save resources

## SSL/TLS Configuration

### PostgreSQL with SSL

```python
config = DatabaseConfig(
    driver=DatabaseType.Postgres,
    url="postgresql://user:password@host:5432/db",
    ssl_mode="require",  # Require SSL connection
    ssl_cert_path="/path/to/client-cert.pem",
    ssl_key_path="/path/to/client-key.pem",
    ssl_ca_path="/path/to/ca-cert.pem",
)
```

## Database-Specific Setup

### PostgreSQL

```bash
# Create database
createdb mydatabase

# Create user
createuser myuser
psql -U postgres -d mydatabase -c "ALTER USER myuser WITH PASSWORD 'password';"

# Grant privileges
psql -U postgres -d mydatabase -c "GRANT ALL PRIVILEGES ON DATABASE mydatabase TO myuser;"
```

### MySQL

```bash
# Create database
mysql -u root -p -e "CREATE DATABASE mydatabase;"

# Create user
mysql -u root -p -e "CREATE USER 'myuser'@'localhost' IDENTIFIED BY 'password';"

# Grant privileges
mysql -u root -p -e "GRANT ALL PRIVILEGES ON mydatabase.* TO 'myuser'@'localhost';"
mysql -u root -p -e "FLUSH PRIVILEGES;"
```

### SQLite

```python
# No setup needed - SQLite is file-based
config = DatabaseConfig(
    driver=DatabaseType.SQLite,
    url="sqlite:///./data/mydatabase.db",
)
DatabaseConnection.connect(config)
```

## Connection Lifecycle

### Initialization

```python
from sqlrustler.sqlrustler import DatabaseConnection

# Initialize connection at application startup
config = DatabaseConfig(...)
DatabaseConnection.connect(config)
```

### Cleanup

```python
# Close connection at application shutdown
DatabaseConnection.disconnect()
```

### Context Manager

```python
from sqlrustler.sqlrustler import DatabaseConnection

# Automatic cleanup
with DatabaseConnection(config) as conn:
    # Perform database operations
    users = User.objects().execute()
    # Connection automatically closed
```

## Troubleshooting

### Connection Refused

```python
# Check configuration
print(f"Host: {config.url}")
print(f"Max connections: {config.max_connections}")

# Test with command line tools
# psql -U user -h localhost -d database
# mysql -u user -h localhost -p database
```

### Authentication Failed

- Verify username and password
- Check database user permissions
- Ensure user is created for the correct host

### Connection Timeout

- Increase `connection_timeout` parameter
- Check network connectivity to database host
- Verify firewall rules allow connection

### Too Many Connections

- Reduce `max_connections` or add `idle_timeout`
- Check for connection leaks in application
- Monitor active connections: `SELECT count(*) FROM pg_stat_activity;`
