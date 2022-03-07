
from postmodel import Postmodel, run_async

from postmodel import models
import asyncio

class Foo(models.Model):
    foo_id = models.IntField(pk=True)

async def create_db():
    await Postmodel.init('postgres://postgres:@127.0.0.1:5432/test_db', modules=[__name__])
    db = Postmodel.get_database()
    print('inited.', type(db))

run_async(create_db())