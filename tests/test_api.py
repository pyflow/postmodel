
from postmodel import Postmodel, models
import pytest
from postmodel.exceptions import (
    IntegrityError,
    StaleObjectError,
    OperationalError,
    DoesNotExist,
    MultipleObjectsReturned,
    PrimaryKeyIntegrityError,
    PrimaryKeyChangedError
)
import asyncio
from postmodel.models import QueryExpression, Q
from postmodel.models import functions as fn
from tests.testmodels import (Foo, Book,
    CharFieldsModel, MultiPrimaryFoo)
from datetime import datetime, date

@pytest.mark.asyncio
async def test_api_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()

    await CharFieldsModel.all().delete()

    m = await CharFieldsModel.create(id=1, char="hello", char_null="hi")
    await CharFieldsModel.bulk_create([
        CharFieldsModel(id=2, char="hello2"),
        CharFieldsModel(id=3, char="hello3", char_null="null3"),
        CharFieldsModel(id=4, char="hello4", char_null="null4"),
        CharFieldsModel(id=5, char="hello5")
    ])

    mlist = await CharFieldsModel.exclude(id = 3)
    assert len(mlist) == 4

    mlist = await CharFieldsModel.filter(id__in = [2, 4])
    assert len(mlist) == 2
    mlist = await CharFieldsModel.exclude(id__not_in = [2, 4])
    assert len(mlist) == 2

    mlist = await CharFieldsModel.exclude(id__in = [2, 4])
    assert len(mlist) == 3

    mlist = await CharFieldsModel.filter(id__not_in = [2, 4])
    assert len(mlist) == 3

    ret = await CharFieldsModel.get_or_none(id = 14)
    assert ret == None

    ret = await CharFieldsModel.all().get_or_none(id=42)
    assert ret == None

    ret = await CharFieldsModel.filter(Q(id__in=[3])).first()
    assert ret.id == 3

    ret = await CharFieldsModel.exclude(Q(id__in=[1,2,3,4])).first()
    assert ret.id == 5

    async for cfm in CharFieldsModel.filter(id__in = [2, 4]):
        assert cfm.id in [2, 4]

    obj, created = await CharFieldsModel.get_or_create(
            id = 5,
            defaults={'char_null': "get_or_create"}
        )
    assert created == False
    assert obj.id == 5
    assert obj.char_null == None

    obj, created = await CharFieldsModel.get_or_create(
            id = 5
        )
    assert created == False
    assert obj.id == 5

    obj, created = await CharFieldsModel.get_or_create(
            id = 25,
            defaults={'char':"get_or_create", 'char_null': "get_or_create_null"}
        )
    assert created == True
    assert obj.id == 25
    assert obj.char == "get_or_create"

    await CharFieldsModel.all().delete()
    await Postmodel.close()


@pytest.mark.asyncio
async def test_api_queries(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    await Postmodel.generate_schemas()

    await CharFieldsModel.bulk_create([
        CharFieldsModel(id=2, char="Hello Moo World"),
        CharFieldsModel(id=3, char="HELLO WWW", char_null="null3"),
        CharFieldsModel(id=4, char="postMODEL works", char_null="null4"),
        CharFieldsModel(id=5, char="aSyNCio rocks"),
        CharFieldsModel(id=6, char="Hello MOO World"),
    ])
    async for obj in CharFieldsModel.filter(char__contains="or"):
        assert "or" in obj.char

    mlist  = await CharFieldsModel.filter(char__icontains="hello")
    assert len(mlist) == 3
    for obj in mlist:
        assert "hello" in obj.char.lower()

    async for obj in CharFieldsModel.filter(char__startswith="He"):
        assert obj.char.startswith("He")

    async for obj in CharFieldsModel.filter(char__istartswith="He"):
        assert obj.char[0:2].lower() == "he"

    async for obj in CharFieldsModel.filter(char__endswith="ks"):
        assert obj.char.endswith("ks")

    async for obj in CharFieldsModel.filter(char__iendswith="KS"):
        assert obj.char[-2:].upper() == "KS"

    async for obj in CharFieldsModel.filter(char__iexact="Hello Moo World"):
        assert obj.char.upper() == "HELLO MOO WORLD"

    async for obj in CharFieldsModel.filter(id__not=3):
        assert obj.id != 3

    async for obj in CharFieldsModel.filter(id__gt=3):
        assert obj.id > 3

    async for obj in CharFieldsModel.filter(id__gte=3):
        assert obj.id >= 3

    async for obj in CharFieldsModel.filter(id__lt=3):
        assert obj.id < 3

    async for obj in CharFieldsModel.filter(id__lte=3):
        assert obj.id <= 3

    async for obj in CharFieldsModel.filter(char_null__isnull=True):
        assert obj.char_null == None

    async for obj in CharFieldsModel.filter(char_null__isnull=False):
        assert obj.char_null != None

    async for obj in CharFieldsModel.filter(char_null__not_isnull=True):
        assert obj.char_null != None

    async for obj in CharFieldsModel.filter(char_null__not_isnull=False):
        assert obj.char_null == None

    await CharFieldsModel.all().delete()

    await Postmodel.close()


@pytest.mark.asyncio
async def test_api_updates(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    assert len(Postmodel._databases) == 1
    assert Postmodel._inited == True
    mapper = Postmodel.get_mapper(Book)
    await mapper.delete_table()

    await Postmodel.generate_schemas()

    await Book.create(id=1, name='learning python', description="learn how to write python program.")
    m = await Book.get(id=1)
    m1 = await Book.get(id=1)
    assert m.data_ver == 1
    assert m1.data_ver == 1
    data = m.to_dict()
    for key in ["id", "name", "description", "created", "updated", "data_ver"]:
        assert data[key] == getattr(m, key)
    json_data = m.to_jsondict()
    for key in ["id", "name", "description", "data_ver"]:
        assert json_data[key] == getattr(m, key)
    assert type(json_data["created"]) == str
    assert json_data["created"] == m.created.isoformat()
    assert json_data["updated"] == m.updated.isoformat()
    m.description = "modified description"
    await m.save()
    m = await Book.get(id=1)
    assert m.data_ver == 2
    m1.description = "third description"
    with pytest.raises(Exception):
        await m1.save()
    await m1.save(force=True)
    m = await Book.get(id=1)
    assert m.description == 'third description'
    assert m.data_ver == 2
    with pytest.raises(PrimaryKeyChangedError):
        m.pk = 3

    m = Book(id=1, name="book1", description="should not in database.")
    with pytest.raises(Exception):
        await m.save()
    await Postmodel.close()

@pytest.mark.asyncio
async def test_api_single_primary_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    mapper = Postmodel.get_mapper(Foo)
    await mapper.delete_table()
    await Postmodel.generate_schemas()

    foo = Foo(foo_id=1, name="n1", tag="n", memo="")
    await foo.save()

    with pytest.raises(PrimaryKeyIntegrityError):
        f = await Foo.load()

    f = await Foo.load(foo_id = 1)
    assert f.name == 'n1'
    assert f.tag == 'n'

    with pytest.raises(PrimaryKeyChangedError):
        foo.foo_id = 2
    await Postmodel.close()

@pytest.mark.asyncio
async def test_api_multi_1(db_url):
    await Postmodel.init(db_url, modules=[__name__])
    mapper = Postmodel.get_mapper(MultiPrimaryFoo)
    await mapper.delete_table()
    await Postmodel.generate_schemas()
    foo = MultiPrimaryFoo(foo_id=1, name="n1", tag="n", date=date.today())
    await foo.save()
    f = await MultiPrimaryFoo.get_or_none(foo_id=1, name="n1")
    assert f.foo_id == 1
    assert f.name == "n1"
    f2 = await MultiPrimaryFoo.get_or_none(foo_id=1, name="n2")
    assert f2 == None

    f = await MultiPrimaryFoo.get_or_none(foo_id=1, name="n1")
    assert f.foo_id == 1
    await f.delete()

    f = await MultiPrimaryFoo.get_or_none(foo_id=1, name="n1")
    assert f == None

    foo = MultiPrimaryFoo(foo_id=1, name="n1", tag="n", date=date.today())
    await foo.save()

    foo.tag = 'tag'
    await foo.save()

    with pytest.raises(PrimaryKeyIntegrityError):
        f = await MultiPrimaryFoo.load(foo_id=1)

    f = await MultiPrimaryFoo.get_or_none(foo_id=1, name="n1")
    assert f.tag == 'tag'
    assert f == foo

    f = await MultiPrimaryFoo.load(foo_id=1, name="n1")
    assert f.tag == 'tag'
    assert f == foo

    with pytest.raises(PrimaryKeyChangedError):
        foo.foo_id = 2

    with pytest.raises(PrimaryKeyChangedError):
        foo.name = "n2"
    await Postmodel.close()
