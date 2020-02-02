
from .engine import BaseSQLDBEngine, BaseSQLDBClient, BaseSQLDBMapper
import asyncio
import asyncpg
from postmodel.exceptions import OperationalError, DBConnectionError

class PostgresClient(BaseSQLDBClient):
    def __init__(self, name,  config, parameters={}):
        super(PostgresClient, self).__init__(name, config=config, parameters=parameters)
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

    async def init(self, with_db=True) -> None:
        if self._pool:
            return
        try:
            self._pool = await asyncpg.create_pool(None, password=self.password, **self._conn_params)
        except asyncpg.InvalidCatalogNameError:
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

class PostgresEngine(BaseSQLDBEngine):
    client_class = PostgresClient
    default_config = {
        'min_size': 1,
        'max_size': 30,
    }