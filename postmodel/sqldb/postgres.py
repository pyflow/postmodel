from typing import Any, List, Optional, Sequence, Tuple, Type, Union
from functools import wraps
from .base import BaseDatabaseEngine, BaseDatabaseMapper
from .base import (TransactedConnections, 
        TransactedConnectionProxy,
        TransactedConnectionWrapper)
import asyncio
import asyncpg
from postmodel.exceptions import (OperationalError, 
        DBConnectionError, 
        IntegrityError, 
        TransactionManagementError,
        MultipleObjectsReturned,
        DoesNotExist)
from postmodel.main import Postmodel
from .common import BaseTableSchemaGenerator, PikaTableFilters
from pypika import Parameter
from pypika import Table, Query, PostgreSQLQuery
from pypika import functions as fn
import operator
from copy import deepcopy


def translate_exceptions(func):
    @wraps(func)
    async def translate_exceptions_(self, *args):
        try:
            return await func(self, *args)
        except asyncpg.SyntaxOrAccessError as exc:
            raise OperationalError(exc)
        except asyncpg.IntegrityConstraintViolationError as exc:
            raise IntegrityError(exc)
        except asyncpg.InvalidTransactionStateError as exc:  # pragma: nocoverage
            raise TransactionManagementError(exc)

    return translate_exceptions_


class PooledTransactionContext:

    __slots__ = ('name', 'token', 'timeout', 'connection', 'transaction', 'done', 'pool')

    def __init__(self, name, pool, timeout):
        self.name = name
        self.pool = pool
        self.timeout = timeout
        self.connection = None
        self.done = False
        self.transaction = None

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise Exception('a connection is already acquired')
        self.connection = await self.pool._acquire(self.timeout)
        self.transaction = self.connection.transaction()
        conn_proxy = TransactedConnectionProxy(self.connection)
        self.token = TransactedConnections.set(self.name, conn_proxy)
        await self.transaction.start()
        return conn_proxy

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            try:
                await self.transaction.rollback()
            except:
                pass
        else:
            await self.transaction.commit()
        self.done = True
        con = self.connection
        self.connection = None
        TransactedConnections.reset(self.name, self.token)
        await self.pool.release(con)


class PostgresMapper(BaseDatabaseMapper):
    EXPLAIN_PREFIX: str = "EXPLAIN"

    def init(self):
        self.meta = self.model_class._meta
        self.pika_table = Table(self.meta.table)
        column_names = []
        columns = []
        self.columns = columns
        self.column_names = column_names
        for name, field in self.meta.fields_map.items():
            column_names.append(name)
            columns.append(field)

        self.filters = PikaTableFilters(self.pika_table, self.meta.filters)

        self.insert_all_sql = str(
            Query.into(self.pika_table)
            .columns(*column_names)
            .insert(*[self.parameter(i) for i in range(len(column_names))]).get_sql()
        )
        self.delete_sql = str(
            Query.from_(self.pika_table).where(
                self.pika_table[self.meta.db_pk_field] == self.parameter(0)
            ).delete().get_sql()
        )
        self.delete_table_sql = str(
            Query.from_(self.pika_table).delete().get_sql()
        )
        self.drop_table_sql = str(
            f"DROP TABLE IF EXISTS {self.meta.table};"
        )
        self.update_cache = {}

    def parameter(self, pos: int) -> Parameter:
        return Parameter("$%d" % (pos + 1,))

    async def explain(self, query) -> Any:
        sql = " ".join((self.EXPLAIN_PREFIX, query.get_sql()))
        return (await self.db.execute_query(sql))[1]

    async def create_table(self):
        sg = BaseTableSchemaGenerator(self.model_class._meta)
        await self.db.execute_script(sg.get_create_schema_sql())
    
    async def clear_table(self):
        await self.db.execute_script(self.delete_table_sql)

    async def delete_table(self):
        await self.db.execute_script(self.drop_table_sql)

    async def insert(self, model_instance):
        values = [
            column.to_db_value(getattr(model_instance, column.model_field_name), model_instance)
            for column in self.columns
        ]
        await self.db.execute_insert(self.insert_all_sql, values)

    async def bulk_insert(self, instances):
        values_list = [
            [
                column.to_db_value(getattr(model_instance, column.model_field_name), model_instance)
                for column in self.columns
            ]
            for model_instance in instances
        ]
        await self.db.execute_many(self.insert_all_sql, values_list)

    def _get_update_sql(self, instance, update_fields, condition_fields={}) -> str:
        """
        Generates the SQL for updating a model depending on provided update_fields.
        Result is cached for performance.
        """
        key = ",".join(update_fields) if update_fields else ""

        condition_keys = list(map(lambda x: x[0], condition_fields))
        if len(condition_keys) > 0:
            key = "{},{}".format(key, ','.join(condition_keys))
        if key in self.update_cache:
            return self.update_cache[key]

        values = []
        table = self.pika_table
        query = PostgreSQLQuery.update(table)
        count = 0
        for field_name in update_fields or self.meta.fields_db_projection.keys():
            db_field = self.meta.fields_db_projection[field_name]
            field_object = self.meta.fields_map[field_name]
            if not field_object.pk:
                query = query.set(table[db_field], self.parameter(count))
                values.append(field_object.to_db_value(getattr(instance, field_name), instance))
                count += 1

        query = query.where(table[self.meta.db_pk_field] == self.parameter(count))
        values.append(self.meta.pk.to_db_value(instance.pk, instance))
        count += 1
        for k, v in condition_fields:
            if k not in (update_fields or []):
                query = query.where(table[k] == self.parameter(count))
                values.append(v)
                count += 1

        sql = self.update_cache[key] = str(query.get_sql())
        return sql, values

    async def update(self, instance, update_fields, condition_fields=[]) -> int:
        sql, values = self._get_update_sql(instance, update_fields, condition_fields)
        ret = await self.db.execute_query(sql, values)
        return ret[0]

    async def delete(self, model_instance):
        ret = await self.db.execute_query(
            self.delete_sql, [self.meta.pk.to_db_value(model_instance.pk, model_instance)]
        )
        return ret[0]

    def _get_query_update_sql(self, updatequery):
        values = []
        table = self.pika_table
        query = PostgreSQLQuery.update(table)
        i = 0
        for expr in updatequery.expressions:
            for key, value in expr.filters.items():
                query = query.where(self.filters.get_criterion(key, self.parameter(i)))
                i += 1
                values.append(value)

        for key, value in updatequery.update_kwargs.items():
            query = query.set(table[key], self.parameter(i))
            values.append(value)
            i += 1
        sql = str(query.get_sql())
        return sql, values

    async def query_update(self, updatequery):
        sql, values= self._get_query_update_sql(updatequery)
        deleted, rows = await self.db.execute_query(sql, values)
        return int(deleted)

    def _get_query_delete_sql(self, deletequery):
        values = []
        table = self.pika_table
        query = PostgreSQLQuery.from_(table)
        i = 0
        for expr in deletequery.expressions:
            for key, value in expr.filters.items():
                query = query.where(self.filters.get_criterion(key, self.parameter(i)))
                i += 1
                values.append(value)
        query = query.delete()
        sql = str(query.get_sql())
        return sql, values

    async def query_delete(self, deletequery):
        sql, values= self._get_query_delete_sql(deletequery)
        deleted, rows = await self.db.execute_query(sql, values)
        return int(deleted)

    def _get_query_count_sql(self, countquery):
        values = []
        table = self.pika_table
        query = PostgreSQLQuery.from_(table).select(fn.Count("*"))
        i = 0
        for expr in countquery.expressions:
            for key, value in expr.filters.items():
                query = query.where(self.filters.get_criterion(key, self.parameter(i)))
                i += 1
                values.append(value)
        sql = str(query.get_sql())
        return sql, values

    async def query_count(self, countquery):
        sql, values= self._get_query_count_sql(countquery)
        _, rows = await self.db.execute_query(sql, values)
        return int(rows[0]['count'])

    def _get_query_sql(self, queryset):
        values = []
        table = self.pika_table
        query = PostgreSQLQuery.from_(table).select(*self.column_names)
        i = 0
        for expr in queryset._expressions:
            for key, value in expr.filters.items():
                query = query.where(self.filters.get_criterion(key, self.parameter(i)))
                i += 1
                values.append(value)
        if queryset._limit:
            query = query.limit(queryset._limit)
        sql = str(query.get_sql())
        return sql, values

    async def query(self, queryset):
        sql, values= self._get_query_sql(queryset)
        _, rows = await self.db.execute_query(sql, values)
        if queryset._expect_single:
            if len(rows) > 1:
                raise MultipleObjectsReturned("Multiple objects returned, expected exactly one")
            elif len(rows) == 0:
                raise DoesNotExist("Object does not exist")
        if queryset._return_single or queryset._expect_single:
            return self.model_class._init_from_db(**rows[0])
        else:
            return [self.model_class._init_from_db(**row) for row in rows]

class PostgresEngine(BaseDatabaseEngine):
    mapper_class = PostgresMapper
    default_config = {
        'min_size': 10,
        'max_size': 30,
    }
     
    def __init__(self, name,  config, parameters={}):
        super(PostgresEngine, self).__init__(name, config=config, parameters=parameters)
        self.user = self.config['username']
        self.password = self.config['password']
        self.database = self.config['db_path']
        self.host = self.config['hostname']
        self.port = int(self.config['port'])
        
        self._conn_params = {
            "host": self.host,
            "port": self.port,
            "user": self.user,
            "database": self.database,
            **self.parameters
            }
        self._pool = None
        self._db_url = f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/'
    
    async def init(self):
        if not self._pool:
            await self._create_pool()

    async def _create_pool(self, create_db=True):
        if self._pool:	
            return	
        try:	
            self._pool = await asyncpg.create_pool(None, password=self.password, **self._conn_params)	
        except asyncpg.InvalidCatalogNameError:
            if create_db:
                await self.db_create()	
                self._pool = await asyncpg.create_pool(None, password=self.password, **self._conn_params)	
        except:	
            raise DBConnectionError(f"Can't establish connection to database {self.database}")
    
    async def close(self) -> None:
        await self._close()
    
    async def _close(self) -> None:
        if self._pool:  # pragma: nobranch
            try:
                await asyncio.wait_for(self._pool.close(), 10)
            except asyncio.TimeoutError:  # pragma: nocoverage
                self._pool.terminate()
            self._pool = None

    async def db_create(self) -> None:
        conn = await asyncpg.connect(self._db_url)
        try:
            await conn.execute(f'CREATE DATABASE "{self.database}" OWNER "{self.user}"')
        except Exception as e:
            raise OperationalError(f"create database {self.database}, error: {str(e)}")
        await conn.close()

    async def db_delete(self) -> None:
        await self.close()
        conn = await asyncpg.connect(self._db_url)
        try:
            await conn.execute(f'DROP DATABASE "{self.database}"')
        except Exception as e:  # pragma: nocoverage
            raise OperationalError(f"drop database {self.database}, error: {str(e)}")
        await conn.close()
    
    def in_transaction(self):
        transacted_conn =  self._current_transacted_conn()
        if transacted_conn:
            raise Exception('nested in_transaction not allowed.')
        else:
            return PooledTransactionContext(self.name, self._pool, timeout=None)
    
    def _current_transacted_conn(self):
        try:
            return TransactedConnections.get(self.name)
        except:
            return None 

    def acquire_connection(self, timeout=None):
        if not self._pool:
            raise Exception('Database init() not called.')
        transacted_conn =  self._current_transacted_conn()
        if transacted_conn:
            return TransactedConnectionWrapper(transacted_conn)
        else:
            return self._pool.acquire(timeout=timeout)
    
    @translate_exceptions
    async def execute_insert(self, query: str, values: list) -> int:
        async with self.acquire_connection() as connection:
            ret = await connection.execute(query, *values)
            try:
                rows_affected = int(ret.split(" ")[-1])
            except Exception:  # pragma: nocoverage
                rows_affected = 0
            return rows_affected
    
    @translate_exceptions
    async def execute_many(self, query: str, values: list) -> None:
        async with self.acquire_connection() as connection:
            async with connection.transaction():
                await connection.executemany(query, values)
    
    @translate_exceptions
    async def execute_query(
        self, query: str, values: Optional[list] = None
    ) -> Tuple[int, List[dict]]:
        async with self.acquire_connection() as connection:
            if values:
                params = [query, *values]
            else:
                params = [query]
            if query.startswith("UPDATE") or query.startswith("DELETE"):
                ret = await connection.execute(*params)
                try:
                    rows_affected = int(ret.split(" ")[1])
                except Exception:  # pragma: nocoverage
                    rows_affected = 0
                return rows_affected, []
            else:
                rows = await connection.fetch(*params)
                return len(rows), rows

    @translate_exceptions
    async def execute_query_dict(self, query: str, values: Optional[list] = None) -> List[dict]:
        async with self.acquire_connection() as connection:
            if values:
                return list(map(dict, await connection.fetch(query, *values)))
            return list(map(dict, await connection.fetch(query)))

    @translate_exceptions
    async def execute_script(self, query: str) -> str:
        async with self.acquire_connection() as connection:
            return await connection.execute(query)