class DBFieldValidationError(Exception):
    pass

class DoesNotExist(Exception):
    """Raised when a query returns no results when exactly one is expected."""
    pass

class MultipleObjectsReturned(Exception):
    """Raised when a query returns multiple results when exactly one is expected."""
    pass
