"""Custom exceptions for SqlRustler ORM."""


class SqlRustlerError(Exception):
    """Base exception for all SqlRustler errors."""
    pass


class DatabaseConnectionError(SqlRustlerError):
    """Raised when database connection fails."""
    pass


class QueryExecutionError(SqlRustlerError):
    """Raised when query execution fails."""
    pass


class TransactionError(SqlRustlerError):
    """Raised when transaction operations fail."""
    pass


class ConfigurationError(SqlRustlerError):
    """Raised when configuration is invalid."""
    pass


class DBFieldValidationError(SqlRustlerError):
    """Raised when field validation fails."""
    pass


class DoesNotExist(SqlRustlerError):
    """Raised when a query returns no results when one was expected."""
    pass


class MultipleObjectsReturned(SqlRustlerError):
    """Raised when a query returns multiple results when one was expected."""
    pass


class MigrationError(SqlRustlerError):
    """Raised when migration operations fail."""
    pass


class CacheError(SqlRustlerError):
    """Raised when cache operations fail."""
    pass
