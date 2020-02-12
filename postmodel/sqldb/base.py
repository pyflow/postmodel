
from typing import Any, List, Optional, Sequence, Tuple, Type, Union, Set
import copy
import asyncio

current_transaction_map: dict = {}

class NestedTransaction:
    async def __aenter__(self):
        pass

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False

class TransactedConnectionProxy:
    def __init__(self, connection):
        self.connection = connection
        self.lock = asyncio.Lock()
    
    def __getattr__(self, attr):
        # Proxy all unresolved attributes to the wrapped Connection object.
        return getattr(self.connection, attr)

    def transaction(self):
        return NestedTransaction()

class TransactedConnections:
    @classmethod
    def set(cls, name, connection):
        return current_transaction_map[name].set(connection)
    
    @classmethod
    def reset(cls, name, token):
        return current_transaction_map[name].reset(token)
    
    @classmethod
    def get(cls, name):
        return current_transaction_map[name].get()


class TransactedConnectionWrapper:
    __slots__ = ("transacted_conn", "lock")

    def __init__(self, transacted_conn) -> None:
        self.transacted_conn = transacted_conn
        self.lock = transacted_conn.lock

    async def __aenter__(self):
        await self.lock.acquire()
        return self.transacted_conn

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        self.lock.release()


class BaseDatabaseMapper(object):
    def __init__(self, model_class, db):
        self.model_class = model_class
        self.db = db
        self.init()

    def init(self):
        raise NotImplementedError()

    async def explain(self, query) -> Any:
        raise NotImplementedError()

    async def create_table(self):
        raise NotImplementedError()
    
    async def clear_table(self):
        raise NotImplementedError()

    async def delete_table(self):
        raise NotImplementedError()

    async def insert(self, model_instance):
        raise NotImplementedError()

    async def bulk_insert(self, instances):
        raise NotImplementedError()

    async def query_update(self, updatequery):
        raise NotImplementedError()

    async def query_delete(self, deletequery):
        raise NotImplementedError()

    async def query_count(self, countquery):
        raise NotImplementedError()

    async def delete(self, model_instance):
        raise NotImplementedError()


class BaseDatabaseEngine(object):
    mapper_class = BaseDatabaseMapper
    default_config = {}
    default_parameters = {}

    def __init__(self, name, config, parameters={}):
        self.name = name
        self.config = copy.deepcopy(self.default_config)
        self.config.update(config)
        self.parameters = copy.deepcopy(self.default_parameters)
        for k, v in self.parameters.items():
            if k in parameters:
                self.parameters[k] = type(v)(parameters[k])


    async def init(self):
        raise NotImplementedError()
    
    async def close(self):
        raise NotImplementedError()
    
    async def db_create(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage

    async def db_delete(self) -> None:
        raise NotImplementedError()  # pragma: nocoverage
    
    def acquire_connection(self):
        raise NotImplementedError()  # pragma: nocoverage
    
    def get_mapper(self, model_class):
        return self.mapper_class(model_class, self)