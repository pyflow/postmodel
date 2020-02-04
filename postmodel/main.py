
import sys

import asyncio
import importlib
import inspect
import warnings
from typing import Any, Coroutine, Dict, List, Optional, Tuple, Type, Union, cast
from basepy.log import logger
from basepy.log import logger


from postmodel.model import Model

from urllib.parse import urlparse, parse_qs
import uuid

from postmodel.exceptions import ConfigurationError
from postmodel.sqldb.base import current_transaction_map

try:
    from contextvars import ContextVar
except ImportError:  # pragma: nocoverage
    from aiocontextvars import ContextVar  # type: ignore


class Postmodel:
    ENGINES_CLASS = {
        'postgres': ('postmodel.sqldb.postgres', 'PostgresEngine'),
    }
    _engines = {}
    _models = {}
    _inited = False

    @classmethod
    def get_engine(cls, egnine_name='default'):
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
            await cls._reset()

        engine_type, config, parameters = cls._parse_db_url(default_db_url)

        db_urls = {}
        db_urls.update(extra_db_urls)
        db_urls['default'] = default_db_url

        for key, value in db_urls.items():
            engine_type, config, parameters = cls._parse_db_url(value)
            cls._engines[key] = await cls._init_engine(key, engine_type, config, parameters)
            current_transaction_map[key] = ContextVar("TransactedConnection", default=None)
        
        print(current_transaction_map)

        for module in modules:
            models = await cls._load_models(module)
            cls._models.update(models)

        cls._inited = True
    

    @classmethod
    def _parse_db_url(cls, db_url):
        url = urlparse(db_url)
        engine_type = url.scheme
        if engine_type not in cls.ENGINES_CLASS:
            raise ConfigurationError(f"Unknown DB scheme: {engine_type}")

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

        return engine_type, config, params

    @classmethod
    async def _init_engine(cls, engine_name, engine_type, config, parameters):
        db_engine_module, db_engine_class = cls.ENGINES_CLASS[engine_type]
        engine_module = importlib.import_module(db_engine_module)

        try:
            engine_class = getattr(engine_module, db_engine_class)  # type: ignore
        except AttributeError:
            raise ConfigurationError(f'Backend for engine "{engine_type}" does not implemented')
        
        engine = engine_class(engine_name, config, parameters)
        await engine.init()
        return engine

    @classmethod
    async def _load_models(cls, module_name):
        module = importlib.import_module(module_name)
        models = {}
        model_names = getattr(module, "__models__", None)

        if model_names:
            if not isinstance(model_names, List):
                raise Exception('__models__ must be list of model names')
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
        for model in cls._models:
            pass
    
    @classmethod
    async def _reset(cls):
        await cls.close_engines()
        cls._engines = {}
        cls._models = {}
        cls._inited = False
    
    @classmethod
    async def close_engines(cls) -> None:
        for engine in cls._engines.values():
            await engine.close()
        cls._engines = {}
    
    @classmethod
    async def close(cls):
        await cls._reset()



