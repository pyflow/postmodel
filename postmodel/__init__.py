
__version__ = '0.1.0'

import sys

if sys.version_info < (3, 6):  # pragma: nocoverage
    raise RuntimeError("Postmodel requires Python 3.6")

import asyncio
import importlib
from typing import Any, Coroutine, Dict, List, Optional, Tuple, Type, Union, cast

from postmodel.model import Model

from urllib.parse import urlparse, parse_qs
import uuid

from postmodel.exceptions import ConfigurationError


class Postmodel:
    ENGINES_CLASS = {
        'postgres': ('postmodel.sqldb.postgres', 'PostgresEngine'),
    }
    _engines = {} 
    _inited = False

    @classmethod
    def get_engine(cls, egnine_name):
        return cls._engines[egnine_name]

    @classmethod
    async def init(
        cls,
        default_db_url,
        _create_db = False,
        extra_db_urls = {},
        modules = [],
    ) -> None:
        if cls._inited:
            await cls.close_engines()

        engine_name, config, parameters = cls._parse_db_url(default_db_url)

        cls._engines['default'] = cls._init_engine(engine_name, config, parameters)

        for key, value in extra_db_urls.items():
            engine_name, config, parameters = cls._parse_db_url(value)
            cls._engines[key] = cls._init_engine(engine_name, config, parameters)

        cls._inited = True
    

    @classmethod
    def _parse_db_url(cls, db_url):
        url = urlparse(db_url)
        engine_name = url.scheme
        if engine_name not in cls.ENGINES_CLASS:
            raise ConfigurationError(f"Unknown DB scheme: {engine_name}")

        config = dict(
            hostname = url.hostname,
            username = url.username or None,
            password = url.password or None
        )
        try:
            if url.port:
                config['port'] = int(url.port)
        except ValueError:
            raise ConfigurationError("Port is not an integer")
        
        config['db_path'] = url.path.lstrip('/')

        params: dict = {}

        for key, ValueError in parse_qs(url.query).items():
            params[key] = value

        return engine_name, config, params

    @classmethod
    def _init_engine(cls, engine_name, config, parameters):
        db_engine_module, db_engine_class = cls.ENGINES_CLASS[engine_name]
        engine_module = importlib.import_module(db_engine_module)

        try:
            engine_class = getattr(engine_module, db_engine_class)  # type: ignore
        except AttributeError:
            raise ConfigurationError(f'Backend for engine "{engine_name}" does not implement db client')

        return engine_class(engine_name, config, parameters)

    @classmethod
    async def close_engines(cls) -> None:
        for engine in cls._engines.values():
            await engine.close()
        cls._engines = {}



def run_async(coro: Coroutine) -> None:
    """
    Simple async runner that cleans up DB connections on exit.
    This is meant for simple scripts.

    Usage::

        from postmodel import Postmodel, run_async

        async def do_stuff():
            await Postmodel.init(
                db_url='sqlite://db.sqlite3',
                modules=['app.models']
            )

            ...

        run_async(do_stuff())
    """
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(coro)
    finally:
        loop.run_until_complete(Postmodel.close_engines())

