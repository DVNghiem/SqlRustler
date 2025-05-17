from abc import ABC, abstractmethod

from .enum import Operator


class DatabaseAdapter(ABC):
    """Adapter for database-specific SQL generation and operator support."""

    @abstractmethod
    def get_placeholder(self, counter: int) -> str:
        pass

    @abstractmethod
    def supports_operator(self, op: str) -> bool:
        pass

    @abstractmethod
    def format_operator(self, op: str) -> str:
        pass


class PostgresAdapter(DatabaseAdapter):
    def get_placeholder(self, counter: int) -> str:
        return f"${counter}"

    def supports_operator(self, op: str) -> bool:
        return op in {e.value for e in Operator}

    def format_operator(self, op: str) -> str:
        return op


class MySqlAdapter(DatabaseAdapter):
    def get_placeholder(self, counter: int) -> str:
        return "?"

    def supports_operator(self, op: str) -> bool:
        unsupported = {"ILIKE", "~", "~*"}
        return op not in unsupported

    def format_operator(self, op: str) -> str:
        if op == "ILIKE":
            return "LIKE"
        return op
