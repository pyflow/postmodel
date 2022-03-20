

from postmodel.sqldb.common import BaseTableSchemaGenerator, FunctionResolve
from postmodel.models.functions import Function, Max, Length, Avg, Coalesce, Trim
from postmodel import models
from pypika import Table
import pytest

class CastFunction(Function):
    pass

class Foo(models.Model):
    foo_id = models.IntField(pk=True)
    name = models.CharField(max_length=255, index=True)
    tag = models.CharField(max_length=128)
    memo = models.TextField()
    content = models.JSONField()
    date = models.DateField()
    updated = models.DatetimeField(auto_now=True)
    created = models.DatetimeField(auto_now_add=True)
    class Meta:
        table = "foo_table"
        unique_together = ('name', 'date')
        indexes = ('name', 'tag')

class MultiPrimaryFoo(models.Model):
    foo_id = models.IntField()
    name = models.CharField(max_length=255)
    tag = models.CharField(max_length=128)
    memo = models.TextField()
    content = models.JSONField()
    date = models.DateField()
    updated = models.DatetimeField(auto_now=True)
    created = models.DatetimeField(auto_now_add=True)
    class Meta:
        table = "multi_primary_foo_table"
        primary_key = ('foo_id', 'name')
        unique_together = ('name', 'date')
        indexes = ('name', 'tag')


def test_table_create_schema_1():
    sg = BaseTableSchemaGenerator(Foo._meta)
    print(sg.get_create_schema_sql())

def test_table_create_schema_2():
    sg = BaseTableSchemaGenerator(MultiPrimaryFoo._meta)
    create_sql = sg.get_create_schema_sql()
    print(create_sql)
    assert 'PRIMARY KEY (foo_id, name)' in create_sql

def test_function_resolve():
    table = Table('test_func_table')
    tc = CastFunction('field_a')
    fr = FunctionResolve(tc)
    with pytest.raises(Exception):
        fr.resolve(table)

    fn = Coalesce('field_c', None, 'abc', Trim('field_b'))
    fr = FunctionResolve(fn)
    ret = fr.resolve(table)
    assert ret.get_sql() == "COALESCE(field_c,NULL,'abc',TRIM(field_b))"
