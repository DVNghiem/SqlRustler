from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List


class DatabaseType(Enum):
    Postgres: str
    MySQL: str
    SQLite: str


@dataclass
class DatabaseConfig:
    driver: DatabaseType
    url: str
    max_connections: int = 10
    min_connections: int = 1
    idle_timeout: int = 30

    options: Dict[str, Any] = {}


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


def get_session_database(context_id: str) -> DatabaseTransaction: ...
