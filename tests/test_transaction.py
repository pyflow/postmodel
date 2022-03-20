from tests.testmodels import Foo
from postmodel.exceptions import OperationalError
from postmodel.transaction import atomic, in_transaction
from postmodel import Postmodel
import pytest

@atomic()
async def atomic_decorated_func():
    foo = Foo(foo_id=15, name="Test", tag="test", memo="atomic")
    await foo.save()
    await Foo.bulk_create([
        Foo(foo_id=14, name="1", tag="b", memo="bulk create rocks"),
        Foo(foo_id=13, name="2", tag="e", memo="bulk create rocks"),
        Foo(foo_id=12, name="1", tag="a", memo="bulk create rocks"),
    ])

@pytest.mark.asyncio
async def test_transaction_1(db_url):
    await Postmodel.init(db_url, modules=["tests.testmodels"])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    await Foo.all().delete()

    foo = await Foo.first()
    assert foo == None

    try:
        async with in_transaction():
            foo = Foo(foo_id=110, name="transaction", tag="b", memo="...")
            await foo.save()
            raise Exception("exception in transaction")
    except:
        pass

    foo = await Foo.filter(foo_id = 110)
    assert len(foo) == 0

    async with in_transaction():
        foo = Foo(foo_id=110, name="transaction", tag="b", memo="...")
        await foo.save()

    foo = await Foo.filter(foo_id = 110)
    assert len(foo) == 1

    async with in_transaction():
        await Foo.bulk_create([
            Foo(foo_id=114, name="1", tag="b", memo="bulk create rocks"),
            Foo(foo_id=115, name="2", tag="e", memo="bulk create rocks"),
            Foo(foo_id=116, name="1", tag="a", memo="bulk create rocks"),
        ])

    foo = await Foo.filter(foo_id__gt = 110)
    assert len(foo) == 3

    await atomic_decorated_func()
    foo = await Foo.filter(foo_id__lt = 20)
    assert len(foo) == 4

    await Foo.all().delete()
    mapper = Postmodel.get_mapper(Foo)
    await mapper.delete_table()
    await Postmodel.close()
