
from postmodel import Postmodel, models
import pytest
from postmodel.exceptions import (
    IntegrityError,
    OperationalError,
    DoesNotExist,
    MultipleObjectsReturned)
import asyncio
from postmodel.models import QueryExpression, Q
from postmodel.models import functions as fn
from tests.testmodels import Foo, Book

@pytest.mark.asyncio
async def test_init_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    await Postmodel.close()

@pytest.mark.asyncio
async def test_init_2(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    await Postmodel.close()

@pytest.mark.asyncio
async def test_mapper_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    mapper = Postmodel.get_mapper(Foo)
    await mapper.clear_table()
    foo = await Foo.create(foo_id=1, name="hello", tag="hi", memo="a long text memo.")
    foo = await Foo.create(foo_id=2, name="hello", tag="hi", memo="a long text memo.")
    with pytest.raises(IntegrityError):
        await Foo.create(foo_id=2, name="hello", tag="hi", memo="a long text memo.")

    foo2 = await Foo.get(foo_id = 1)
    print("foo2>>", foo2)
    #await asyncio.sleep(6)

    count = await Foo.get(foo_id = 1).count()
    assert count == 1
    count = await Foo.get(name = "hello").count()
    assert count == 2

    count = await Foo.get(name = "hello").delete()
    assert count == 2

    foo = await Foo.create(foo_id=3, name="hello3", tag="low", memo="3 is magic number")
    foo.tag = "low3"
    await foo.save()
    await foo.save()
    with pytest.raises(DoesNotExist):
        foo = await Foo.get(foo_id = 1121)

    foo = await Foo.get(foo_id=3)
    assert foo.tag == 'low3'
    count = await Foo.all().count()
    assert count == 1
    print('count ', count)
    explain_ret = await Foo.all().explain()
    print('explain ret:', explain_ret)
    assert len(explain_ret)  > 0
    ret = await foo.delete()
    print("delete ret>>", ret)
    assert ret == 1

    await Foo.bulk_create([
        Foo(foo_id=4, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=5, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=6, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=7, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=8, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=9, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=10, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=11, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=12, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=13, name="bulk_create", tag="high", memo="bulk create rocks"),
        Foo(foo_id=14, name="bulk_create", tag="high", memo="bulk create rocks")
    ])
    count = await Foo.all().count()
    assert count == 11

    with pytest.raises(MultipleObjectsReturned):
        foo = await Foo.get(foo_id__gt = 0)

    await Foo.filter(name="bulk_create").update(tag="bulk_high")

    for foo in await Foo.filter(name="bulk_create"):
        assert foo.tag == 'bulk_high'
    
    await mapper.delete_table()
    await Postmodel.close()

@pytest.mark.asyncio
async def test_mapper_complex_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()
    mapper = Postmodel.get_mapper(Foo)
    await mapper.clear_table()

    await Foo.bulk_create([
        Foo(foo_id=4, name="1", tag="b", memo="bulk create rocks"),
        Foo(foo_id=5, name="2", tag="e", memo="bulk create rocks"),
        Foo(foo_id=6, name="1", tag="a", memo="bulk create rocks"),
        Foo(foo_id=7, name="4", tag="c", memo="bulk create rocks"),
        Foo(foo_id=8, name="3", tag="f", memo="bulk create rocks"),
        Foo(foo_id=9, name="2", tag="g", memo="bulk create rocks"),
        Foo(foo_id=10, name="4", tag="j", memo="bulk create rocks"),
        Foo(foo_id=11, name="3", tag="h", memo="bulk create rocks"),
        Foo(foo_id=12, name="4", tag="o", memo="bulk create rocks"),
    ])
    foo_list = await Foo.all().order_by("name", "-tag")
    assert len(foo_list) == 9

    first = foo_list[0]
    assert first.foo_id == 4
    assert foo_list[1].foo_id == 6
    assert foo_list[2].foo_id == 9
    assert foo_list[-1].foo_id == 7

    foo_list = await Foo.all().order_by("name", "-tag").limit(4)
    assert len(foo_list) == 4
    assert foo_list[-1].foo_id == 5

    foo_list = await Foo.all().order_by("name", "-tag").offset(2).limit(4)
    assert len(foo_list) == 4
    assert foo_list[-1].foo_id == 8

    foo_list = await Foo.all().order_by("name", "-tag").distinct().offset(2).limit(4)
    assert len(foo_list) == 4
    assert foo_list[-1].foo_id == 8

    await Postmodel.close()

@pytest.mark.asyncio
async def test_mapper_get_criterion(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    mapper = Postmodel.get_mapper(Foo)
    q1 = Q(foo_id__gt = 1)
    q2 = Q(name = "bulk")
    q = q1 & q2
    assert q1.children == ()
    criterion, values = mapper._expressions_to_criterion([q], 0)
    sql = criterion.get_sql()
    assert sql == '"foo_id">$1 AND "name"=$2'
    assert values == [1, "bulk"]
    qq = q1 | q2
    print('qq:', type(qq), qq.join_type, qq.filters, qq.children)
    criterion, values = mapper._expressions_to_criterion([qq], 1)
    sql = criterion.get_sql()
    assert sql == '"foo_id">$2 OR "name"=$3'
    assert values == [1, "bulk"]
    
    with pytest.raises(OperationalError):
        q = Q(foo_id__gt = 10)
        q.join_type = "XOR"
        criterion, values = mapper._expressions_to_criterion([q], 1)

    await Postmodel.close()

@pytest.mark.asyncio
async def test_mapper_functions_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    await Postmodel.generate_schemas()
    mapper = Postmodel.get_mapper(Foo)
    await mapper.clear_table()
    await Foo.bulk_create([
        Foo(foo_id=4, name="High", tag="high", memo="bulk create rocks"),
        Foo(foo_id=5, name="meDiUM", tag="medium", memo="bulk create rocks"),
        Foo(foo_id=6, name="LOW", tag="low", memo="bulk create rocks"),
        Foo(foo_id=7, name="HiGH", tag="high", memo="bulk create rocks")
    ])
    count = await Foo.all().count()
    assert count == 4

    foo = await Foo.all().filter(name = fn.Upper('tag'))
    assert len(foo) == 1
    foo = foo[0]
    assert foo.foo_id == 6

    await Postmodel.close()