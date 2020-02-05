
from postmodel import Postmodel
import pytest
from postmodel import Model, fields
from basepy.log import logger

logger.add('stdout')

@pytest.mark.asyncio
async def test_init_1():
    await Postmodel.init('postgres://postgres@localhost:54320/test_db', modules=[__name__])
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
    # await db.execute_script('''
    #     DROP TABLE "test_db_report";
    # ''')
    await Postmodel.close()
