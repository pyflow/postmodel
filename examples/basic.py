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

class Book(models.Model):
    id = models.IntField(pk=True)
    name = models.TextField()
    tag = models.CharField(max_length=120)

    class Meta:
        table = "book_test"

    def __str__(self):
        return self.name

async def run():
    await Postmodel.init('postgres://postgres@localhost:5432/test_db', modules=[__name__])
    await Postmodel.generate_schemas()

    await Event.all().delete()

    event = await Event.create(id=1, name="Test")
    await Event.filter(id=event.id).update(name="Updated name")

    print(await Event.filter(name="Updated name").first())
    # >>> Updated name

    await Event(id=2, name="Test 2").save()

    await Book.all().delete()
    # Create instance by save
    book = Book(id=1, name='Mastering postmdel', tag="orm")
    await book.save()

    # Or by .create()
    await Book.create(id=2, name='Learning Python', tag="python")

    # Query

    books = await Book.filter(tag="orm").all()
    assert len(books) == 1


if __name__ == "__main__":
    run_async(run())
