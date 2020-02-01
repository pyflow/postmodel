
__version__ = '0.1.0'

import sys

if sys.version_info < (3, 6):  # pragma: nocoverage
    raise RuntimeError("Postmodel requires Python 3.6")

import asyncio
from typing import Any, Coroutine, Dict, List, Optional, Tuple, Type, Union, cast

from postmodel.model import Model

import urllib.parse as urlparse
import uuid

from postmodel.exceptions import ConfigurationError


class Postmodel:
    _connections = {} 
    _inited = False

    @classmethod
    def get_connection(cls, connection_name):
        """
        Returns the connection by name.

        :raises KeyError: If connection name does not exist.
        """
        return cls._connections[connection_name]

    @classmethod
    async def init(
        cls,
        default_db_url,
        _create_db = False,
        extra_db_urls = {},
        modules = [],
    ) -> None:
        if cls._inited:
            await cls.close_connections()

        cls._connections['default'] = default_db_url

        cls._connections.update(extra_db_urls)

        cls._inited = True

    @classmethod
    async def close_connections(cls) -> None:
        """
        Close all connections cleanly.

        It is required for this to be called on exit,
        else your event loop may never complete
        as it is waiting for the connections to die.
        """
        for connection in cls._connections.values():
            await connection.close()
        cls._connections = {}



def run_async(coro: Coroutine) -> None:
    """
    Simple async runner that cleans up DB connections on exit.
    This is meant for simple scripts.

    Usage::

        from postmodel import Postmodel, run_async

        async def do_stuff():
            await Postmodel.init(
                db_url='sqlite://db.sqlite3',
                models={'models': ['app.models']}
            )

            ...

        run_async(do_stuff())
    """
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(Postmodel.close_connections())

