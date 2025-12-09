"""
Test suite for SqlRustler exception handling.
"""
import pytest
from sqlrustler.exceptions import (
    SqlRustlerError,
    DatabaseConnectionError,
    QueryExecutionError,
    TransactionError,
    ConfigurationError,
    DBFieldValidationError,
    DoesNotExist,
    MultipleObjectsReturned,
    MigrationError,
    CacheError,
)


class TestExceptionHierarchy:
    """Test exception class hierarchy."""
    
    def test_all_exceptions_inherit_from_base(self):
        """Test that all custom exceptions inherit from SqlRustlerError."""
        exceptions = [
            DatabaseConnectionError,
            QueryExecutionError,
            TransactionError,
            ConfigurationError,
            DBFieldValidationError,
            DoesNotExist,
            MultipleObjectsReturned,
            MigrationError,
            CacheError,
        ]
        
        for exc_class in exceptions:
            assert issubclass(exc_class, SqlRustlerError)
            assert issubclass(exc_class, Exception)
    
    def test_exceptions_can_be_raised(self):
        """Test that exceptions can be raised and caught."""
        with pytest.raises(DoesNotExist):
            raise DoesNotExist("Test error")
        
        with pytest.raises(SqlRustlerError):
            raise DatabaseConnectionError("Connection failed")
    
    def test_exception_messages(self):
        """Test that exception messages are preserved."""
        msg = "Custom error message"
        try:
            raise DBFieldValidationError(msg)
        except DBFieldValidationError as e:
            assert str(e) == msg


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
