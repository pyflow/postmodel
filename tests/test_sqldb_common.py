

from postmodel.sqldb.common import BaseTableSchemaGenerator
from postmodel import Model, fields

class Foo(Model):
    foo_id = fields.IntField(pk=True)

def test_table_create_schema_1():
    sg = BaseTableSchemaGenerator(Foo._meta)
    print(sg.get_create_schema_sql())