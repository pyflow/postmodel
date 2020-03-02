
import sys

import asyncio
import importlib
import inspect
import warnings
from typing import Any, Coroutine, Dict, List, Optional, Tuple, Type, Union, cast
from basepy.asynclog import logger

from urllib.parse import urlparse, parse_qs
import uuid

from postmodel.exceptions import ConfigurationError
from postmodel.sqldb.base import current_transaction_map

try:
    from contextvars import ContextVar
except ImportError:  # pragma: nocoverage
    from aiocontextvars import ContextVar  # pragma: nocoverage


class Postmodel:
    DATABASE_CLASS = {
        'postgres': ('postmodel.sqldb.postgres', 'PostgresEngine'),
        'postgresql': ('postmodel.sqldb.postgres', 'PostgresEngine'),
    }
    _databases = {}
    _mapper_cache = {}
    _models = {}
    _inited = False

    @classmethod
    def get_database(cls, db_name='default'):
        name = db_name or 'default'
        return cls._databases[name]

    @classmethod
    async def init(
        cls,
        default_db_url,
        extra_db_urls = {},
        modules = [],
        _create_db = False
    ) -> None:
        if cls._inited:
            await cls._reset()

        db_type, config, parameters = cls._parse_db_url(default_db_url)

        db_urls = {}
        db_urls.update(extra_db_urls)
        db_urls['default'] = default_db_url

        for key, value in db_urls.items():
            db_type, config, parameters = cls._parse_db_url(value)
            cls._databases[key] = await cls._init_database(key, db_type, config, parameters)
            current_transaction_map[key] = ContextVar("TransactedConnection", default=None)

        for module in modules:
            models = await cls._load_models(module)
            cls._models.update(models)

        cls._inited = True


    @classmethod
    def _parse_db_url(cls, db_url):
        url = urlparse(db_url)
        db_type = url.scheme
        if db_type not in cls.DATABASE_CLASS:
            raise ConfigurationError(f"Unknown DB scheme: {db_type}")

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

        for key, value in parse_qs(url.query).items():
            params[key] = value[0]

        return db_type, config, params

    @classmethod
    async def _init_database(cls, db_name, db_type, config, parameters):
        db_module_name, db_class_name = cls.DATABASE_CLASS[db_type]
        db_module = importlib.import_module(db_module_name)

        try:
            database_class = getattr(db_module, db_class_name)  # type: ignore
        except AttributeError:
            raise ConfigurationError(f'Backend for database "{db_type}" does not implemented')

        db = database_class(db_name, config, parameters)
        await db.init()
        return db

    @classmethod
    async def _load_models(cls, module_name):
        Model = getattr(importlib.import_module('postmodel.models.model'), 'Model')
        module = importlib.import_module(module_name)
        models = {}
        model_names = getattr(module, "__models__", None)

        if model_names:
            if not isinstance(model_names, List):
                raise ConfigurationError('__models__ must be list of model names')
            possible_models = [(model_name, getattr(module, model_name)) for model_name in model_names]
        else:
            possible_models = [(attr_name, getattr(module, attr_name)) for attr_name in dir(module)]

        for name, value in possible_models:
            if inspect.isclass(value) and issubclass(value, Model) and not value._meta.abstract:
                models['{}.{}'.format(module_name, name)] = value

        if not models:
            await logger.warning(f'Module "{module_name}" has no models')

        return models

    @classmethod
    def get_mapper(cls, model_class, db_name='default'):
        key = (model_class, db_name)
        if key not in cls._mapper_cache:
            db = cls.get_database(db_name)
            mapper = db.get_mapper(model_class)
            cls._mapper_cache[key] = mapper
            return mapper
        else:
            return cls._mapper_cache[key]

    @classmethod
    async def generate_schemas(cls, safe = True) -> None:
        """
        Generate schemas according to models provided to ``.init()`` method.
        Will fail if schemas already exists, so it's not recommended to be used as part
        of application workflow

        Parameters
        ----------
        safe:
            When set to true, creates the table only when it does not already exist.
        """
        if not cls._inited:
            raise ConfigurationError("You have to call .init() first before generating schemas")
        for name, model in cls._models.items():
            mapper = model.get_mapper()
            await mapper.create_table()

    @classmethod
    async def _reset(cls):
        await cls.close_databases()
        cls._databases = {}
        cls._mapper_cache = {}
        cls._models = {}
        cls._inited = False

    @classmethod
    async def close_databases(cls) -> None:
        for db in cls._databases.values():
            await db.close()
        cls._databases = {}

    @classmethod
    async def close(cls):
        await cls._reset()



