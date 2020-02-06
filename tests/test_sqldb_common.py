

from postmodel.sqldb.common import BaseTableSchemaGenerator
from postmodel import models


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

def test_table_create_schema_1():
    sg = BaseTableSchemaGenerator(Foo._meta)
    print(sg.get_create_schema_sql())