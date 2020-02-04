
from postmodel import Postmodel, run_async

from postmodel import Model, fields
import asyncio

class Foo(Model):
    foo_id = fields.IntField(pk=True)

async def create_db():
    await Postmodel.init('postgres://postgres:@127.0.0.1:54320/test_db', modules=[__name__])
    engine = Postmodel.get_engine()
    print('inited.', type(engine))

run_async(create_db())