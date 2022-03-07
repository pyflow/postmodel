
from postmodel import Postmodel
import pytest
from postmodel import models
from basepy.asynclog import logger
from postmodel.sqldb.postgres import PostgresEngine
from postmodel.exceptions import DBConnectionError

logger.add('stdout')

@pytest.mark.asyncio
async def test_database_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    db = Postmodel.get_database()
    assert db != None
    await db.execute_script('''
        DROP TABLE IF EXISTS "test_db_report";
    ''')
    create_sql = '''CREATE TABLE IF NOT EXISTS "test_db_report" (
        "report_id" INT NOT NULL PRIMARY KEY,
        "tag" TEXT NOT NULL,
        "content" TEXT NOT NULL
    ); '''
    await db.execute_script(create_sql)
    await db.execute_insert(
        'INSERT INTO test_db_report (report_id, tag, content)'
        'VALUES($1, $2, $3)'
    , [1, "hello", "hello world"])
    await db.execute_many(
        'UPDATE test_db_report SET content = $1 where report_id = $2',
        [("hello hello world", 1), ("final hello", 1)]
    )
    async with db.in_transaction():
        await db.execute_insert(
            'INSERT INTO test_db_report (report_id, tag, content)'
            'VALUES($1, $2, $3)'
        , [2, "hello", "hello world"])
        await db.execute_many(
        'UPDATE test_db_report SET content = $1 where report_id = $2',
        [("hello hello world in transaction", 1), ("final hello in transaction", 1)]
        )

    ret = await db.execute_query_dict(
        'SELECT * from test_db_report', []
    )
    assert len(ret) == 2
    ret = await db.execute_query_dict(
        'SELECT * from test_db_report where report_id<$1', [10]
    )
    assert len(ret) == 2
    await db.execute_script('''
        DROP TABLE "test_db_report";
    ''')
    await Postmodel.close()

@pytest.mark.asyncio
async def test_database_2():
    config = {
        'username': 'postgres',
        'password': 'postgres',
        'db_path': 'test_db_2',
        'hostname': '127.0.0.1',
        'port': 5432
    }
    db = PostgresEngine('test', config=config)
    await db.db_delete()
    with pytest.raises(Exception):
        db.acquire_connection()
    with pytest.raises(DBConnectionError):
        await db.init(create_db=False)
    await db.init()
    assert db._pool != None
    pool = db._pool
    await db.init()
    assert db._pool == pool
    await db._create_pool()
    assert db._pool == pool
    await db.db_delete()

@pytest.mark.asyncio
async def test_database_transaction(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    db = Postmodel.get_database()
    assert db != None
    async with db.in_transaction():
        with pytest.raises(Exception):
            db.in_transaction()
    await Postmodel.close()