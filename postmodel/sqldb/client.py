import asyncio
import logging
from typing import Any, List, Optional, Sequence, Tuple, Type, Union, Set

from pypika import Query

class BaseSQLDBClient:
    def __init__(self, connection_name: str, **kwargs) -> None:
        self.connection_name = connection_name

    async def create_connection(self, with_db: bool) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def close(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def db_create(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def db_delete(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    def acquire_connection(self):
        raise NotImplementedError()  # pragma: nocoverage

    def _in_transaction(self):
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_insert(self, query: str, values: list) -> Any:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_query(
        self, query: str, values: Optional[list] = None
    ) -> Tuple[int, Sequence[dict]]:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_script(self, query: str) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_many(self, query: str, values: List[list]) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def execute_query_dict(self, query: str, values: Optional[list] = None) -> List[dict]:
        raise NotImplementedError()  # pragma: nocoverage


