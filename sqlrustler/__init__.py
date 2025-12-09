"""SqlRustler - A high-performance Python ORM with Rust bindings.

Provides a Django-inspired API for database operations with PostgreSQL, MySQL, and SQLite.
"""

from .enum import JoinType
from .exceptions import (
    CacheError,
    ConfigurationError,
    DatabaseConnectionError,
    DBFieldValidationError,
    DoesNotExist,
    MigrationError,
    MultipleObjectsReturned,
    QueryExecutionError,
    SqlRustlerError,
    TransactionError,
)
from .express import Expression
from .F import F
from .field import (
    ArrayField,
    BooleanField,
    CharField,
    DateField,
    DateTimeField,
    DecimalField,
    Field,
    FloatField,
    ForeignKeyField,
    IntegerField,
    JSONField,
    TextField,
)
from .model import Model
from .Q import Q
from .queryset import QuerySet
from .shortcuts import (
    bulk_create_or_update,
    get_list_or_404,
    get_object_or_404,
    Http404,
)
from .sqlrustler import (
    DatabaseConfig,
    DatabaseConnection,
    DatabaseType,
    get_db_type_with_alias,
    Session,
)
from .validators import (
    EmailValidator,
    MaxLengthValidator,
    MaxValueValidator,
    MinLengthValidator,
    MinValueValidator,
    RangeValidator,
    RegexValidator,
    URLValidator,
    validate_email,
    validate_url,
    ValidationError,
)
from .cache import (
    QueryCache,
    get_cache,
    configure_cache,
    clear_cache,
    get_cache_stats,
)

__all__ = [
    # Core
    "Model",
    "QuerySet",
    "Field",
    "F",
    "Q",
    "Expression",
    "JoinType",
    # Database
    "DatabaseConfig",
    "DatabaseConnection",
    "DatabaseType",
    "Session",
    "get_db_type_with_alias",
    # Fields
    "IntegerField",
    "CharField",
    "TextField",
    "FloatField",
    "BooleanField",
    "DateTimeField",
    "DateField",
    "JSONField",
    "ArrayField",
    "DecimalField",
    "ForeignKeyField",
    # Exceptions
    "SqlRustlerError",
    "DatabaseConnectionError",
    "QueryExecutionError",
    "TransactionError",
    "ConfigurationError",
    "DBFieldValidationError",
    "DoesNotExist",
    "MultipleObjectsReturned",
    "MigrationError",
    "CacheError",
    # Shortcuts
    "get_object_or_404",
    "get_list_or_404",
    "bulk_create_or_update",
    "Http404",
    # Validators
    "EmailValidator",
    "URLValidator",
    "MinLengthValidator",
    "MaxLengthValidator",
    "MinValueValidator",
    "MaxValueValidator",
    "RangeValidator",
    "RegexValidator",
    "validate_email",
    "validate_url",
    "ValidationError",
    # Cache
    "QueryCache",
    "get_cache",
    "configure_cache",
    "clear_cache",
    "get_cache_stats",
]