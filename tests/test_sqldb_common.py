

from postmodel.sqldb.common import BaseTableSchemaGenerator
from postmodel import Model, fields


class Foo(Model):
    foo_id = fields.IntField(pk=True)
    name = fields.CharField(max_length=255, index=True)
    tag = fields.CharField(max_length=128)
    memo = fields.TextField()
    content = fields.JSONField()
    date = fields.DateField()
    updated = fields.DatetimeField(auto_now=True)
    created = fields.DatetimeField(auto_now_add=True)
    class Meta:
        table = "foo_table"
        unique_together = ('name', 'date')
        indexes = ('name', 'tag')

def test_table_create_schema_1():
    sg = BaseTableSchemaGenerator(Foo._meta)
    print(sg.get_create_schema_sql())