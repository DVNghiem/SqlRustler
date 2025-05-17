from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List


class DatabaseType(Enum, str):
    Postgres = "POSTGRESQL"
    MySQL = "MYSQL"
    SQLite = "SQLITE"


@dataclass
class DatabaseConfig:
    driver: DatabaseType
    url: str
    max_connections: int = 10
    min_connections: int = 1
    idle_timeout: int = 30

    options: Dict[str, Any] = {}

@dataclass
class DatabaseConnection:

    @staticmethod
    def connect(config: DatabaseConfig): ...


@dataclass
class DatabaseTransaction:
    def execute(self, query: str, params: List[Any]) -> int: ...
    def fetch_all(self, query: str, params: List[Any]) -> List[Dict[str, Any]]: ...
    def stream_data(
        self, query: str, params: List[Any], chunk_size: int
    ) -> Dict[str, Any]: ...
    def bulk_change(
        self, query: str, params: List[List[Any]], batch_size: int
    ) -> int | None: ...
    def commit(self) -> None: ...
    def rollback(self) -> None: ...

@dataclass
class TransactionWrapper:

    def execute(self, query: str, params: List[Any]) -> int: ...
    def fetch_all(self, query: str, params: List[Any]) -> List[Dict[str, Any]]: ...
    def stream_data(
        self, query: str, params: List[Any], chunk_size: int
    ) -> Dict[str, Any]: ...
    def bulk_change(
        self, query: str, params: List[List[Any]], batch_size: int
    ) -> int | None: ...


@dataclass
class Session:
    def __enter__(self) -> TransactionWrapper: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
   