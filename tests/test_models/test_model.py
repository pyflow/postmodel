
from postmodel import models
import pytest
from postmodel.exceptions import OperationalError, ConfigurationError, PrimaryKeyChangedError
from tests.testmodels import MultiPrimaryFoo

def test_model_1():
    class NotModelClass:
        pass

    class Foo(models.Model):
        foo_id = models.IntField(pk=True)
        content = models.TextField()

    assert Foo._meta != None
    assert len(Foo._meta.fields_map) == 2

    class FooBar(Foo):
        bar_content = models.TextField()

    assert len(FooBar._meta.fields_map) == 3

    class ModelClassFromNotModel(NotModelClass, models.Model):
        id = models.AutoField()
        name = models.TextField()

    assert ModelClassFromNotModel._meta != None

    with pytest.raises(Exception):
        class DuplicatePKModel(ModelClassFromNotModel):
            dp_id = models.IntField(pk=True)

    with pytest.raises(Exception):
        class DuplicatedPrimaryKeyModel(models.Model):
            dp_id = models.IntField(pk=True)
            dp_no = models.AutoField()


@pytest.mark.asyncio
async def test_model_2():
    class Foo(models.Model):
        id = models.IntField(pk=True)
        content = models.TextField()
    with pytest.raises(ValueError):
        foo = Foo(id=1, content=None)

    foo = Foo(id=1, content="hello", tag="hi")
    assert foo._saved_in_db == False
    with pytest.raises(OperationalError):
        await foo.delete()
    with pytest.raises(PrimaryKeyChangedError):
        foo.pk = 2

    assert hash(foo) == hash(1)

    foo2 = Foo(id=2, content="2")
    assert foo != foo2
    assert foo != "hello"

    class FooNoPK(models.Model):
        id = models.IntField()

        class Meta:
            abstract = True

    foo2 = FooNoPK(id=3)

    with pytest.raises(ConfigurationError):
        class IndexesNotListModel(models.Model):
            id = models.AutoField()
            name = models.TextField()
            class Meta:
                indexes = 'name'

    with pytest.raises(ConfigurationError):
        class IndexesListNotRightModel(models.Model):
            id = models.AutoField()
            name = models.TextField()
            class Meta:
                indexes = (1, 2, 3, 4)

    with pytest.raises(ConfigurationError):
        class IndexesContainWrongFieldModel(models.Model):
            id = models.AutoField()
            name = models.TextField()
            class Meta:
                indexes = ("name", "tag")

    with pytest.raises(Exception):
        class NotAbastractNoPKModel(models.Model):
            id = models.IntField()

    with pytest.raises(Exception):
        class MultiDataversionFieldModel(models.Model):
            id = models.IntField(pk=True)
            data_v1 = models.DataVersionField()
            data_v2 = models.DataVersionField()

