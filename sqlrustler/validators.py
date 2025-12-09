"""
Built-in validators for field validation.

Provides common validators for email, URL, string length, numeric ranges, etc.
"""
import re
from typing import Any, Optional


class ValidationError(Exception):
    """Raised when validation fails."""
    pass


class EmailValidator:
    """Validate email address format."""
    
    # Simple email regex pattern
    pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    
    def __init__(self, message: Optional[str] = None):
        self.message = message or "Enter a valid email address"
    
    def __call__(self, value: str) -> None:
        if not isinstance(value, str) or not self.pattern.match(value):
            raise ValidationError(self.message)


class URLValidator:
    """Validate URL format."""
    
    pattern = re.compile(
        r'^https?://'  # http:// or https://
        r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
        r'localhost|'  # localhost...
        r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
        r'(?::\d+)?'  # optional port
        r'(?:/?|[/?]\S+)$', re.IGNORECASE
    )
    
    def __init__(self, message: Optional[str] = None):
        self.message = message or "Enter a valid URL"
    
    def __call__(self, value: str) -> None:
        if not isinstance(value, str) or not self.pattern.match(value):
            raise ValidationError(self.message)


class MinLengthValidator:
    """Validate minimum string length."""
    
    def __init__(self, min_length: int, message: Optional[str] = None):
        self.min_length = min_length
        self.message = message or f"Ensure this value has at least {min_length} characters"
    
    def __call__(self, value: str) -> None:
        if not isinstance(value, str) or len(value) < self.min_length:
            raise ValidationError(self.message)


class MaxLengthValidator:
    """Validate maximum string length."""
    
    def __init__(self, max_length: int, message: Optional[str] = None):
        self.max_length = max_length
        self.message = message or f"Ensure this value has at most {max_length} characters"
    
    def __call__(self, value: str) -> None:
        if isinstance(value, str) and len(value) > self.max_length:
            raise ValidationError(self.message)


class MinValueValidator:
    """Validate minimum numeric value."""
    
    def __init__(self, min_value: float, message: Optional[str] = None):
        self.min_value = min_value
        self.message = message or f"Ensure this value is greater than or equal to {min_value}"
    
    def __call__(self, value: Any) -> None:
        try:
            if float(value) < self.min_value:
                raise ValidationError(self.message)
        except (TypeError, ValueError):
            raise ValidationError("Value must be numeric")


class MaxValueValidator:
    """Validate maximum numeric value."""
    
    def __init__(self, max_value: float, message: Optional[str] = None):
        self.max_value = max_value
        self.message = message or f"Ensure this value is less than or equal to {max_value}"
    
    def __call__(self, value: Any) -> None:
        try:
            if float(value) > self.max_value:
                raise ValidationError(self.message)
        except (TypeError, ValueError):
            raise ValidationError("Value must be numeric")


class RangeValidator:
    """Validate numeric value is within a range."""
    
    def __init__(self, min_value: float, max_value: float, message: Optional[str] = None):
        self.min_value = min_value
        self.max_value = max_value
        self.message = message or f"Ensure this value is between {min_value} and {max_value}"
    
    def __call__(self, value: Any) -> None:
        try:
            num_value = float(value)
            if not (self.min_value <= num_value <= self.max_value):
                raise ValidationError(self.message)
        except (TypeError, ValueError):
            raise ValidationError("Value must be numeric")


class RegexValidator:
    """Validate value matches a regular expression."""
    
    def __init__(self, pattern: str, message: Optional[str] = None, flags: int = 0):
        self.pattern = re.compile(pattern, flags)
        self.message = message or f"Value must match pattern: {pattern}"
    
    def __call__(self, value: str) -> None:
        if not isinstance(value, str) or not self.pattern.match(value):
            raise ValidationError(self.message)


# Convenience instances
validate_email = EmailValidator()
validate_url = URLValidator()
