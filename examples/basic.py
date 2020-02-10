"""
This example demonstrates most basic operations with single model
"""
from postmodel import Postmodel, models, run_async


class Event(models.Model):
    id = models.IntField(pk=True)
    name = models.TextField()
    datetime = models.DatetimeField(null=True)

    class Meta:
        table = "event"

    def __str__(self):
        return self.name


async def run():
    await Postmodel.init('postgres://postgres@localhost:54320/test_db', modules=[__name__])
    await Postmodel.generate_schemas()

    await Event.all().delete()
    
    event = await Event.create(id=1, name="Test")
    await Event.filter(id=event.id).update(name="Updated name")

    print(await Event.filter(name="Updated name").first())
    # >>> Updated name

    await Event(id=2, name="Test 2").save()


if __name__ == "__main__":
    run_async(run())
