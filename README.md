# postmodel

## Introduction

Postmodel is an easy-to-use ``asyncio`` ORM *(Object Relational Mapper)* inspired by Django and Tortoise ORM.

Postmodel provides 90% Django ORM like API, to ease the migration of developers wishing to switch to ``asyncio``.

Currently, Postmodel provides following features:

* full active-record pattern
* optimistic locking
* 100% code coverage


But, it still have some limits:

* only support Postgresql
* no planing support SQLite, instead it will supports RediSQL
* no support relation


Postmodel is supported on CPython >= 3.6 for PostgreSQL.


## Getting Started

### Installation

You have to install postmodel like this:

```
pip install postmodel
```

### Quick Tutorial

Primary entity of postmodel is ``postmodel.models.Model``.
You can start writing models like this:


```python
from postmodel import models

class Book(models.Model):
    id = models.IntField(pk=True)
    name = models.TextField()
    tag = models.CharField(max_length=120)

    class Meta:
        table = "book_test"

    def __str__(self):
        return self.name
```

After you defined all your models, postmodel needs you to init them, in order to create backward relations between models and match your db client with appropriate models.

You can do it like this:

```python
from postmodel import Postmodel

async def init():
    # Here we connect to a PostgreSQL DB.
    # also specify the app name of "models"
    # which contain models from "app.models"
    await Postmodel.init(
        'postgres://postgres@localhost:54320/test_db',
        modules= [__name__]
    )
    # Generate the schema
    await Postmodel.generate_schemas()
```


Here we create connection to Postgres database, and then we discover & initialise models.

Postmodel currently supports the following databases:

* PostgreSQL (requires ``asyncpg``)

``generate_schema`` generates the schema on an empty database. Postmodel generates schemas in safe mode by default which
includes the ``IF NOT EXISTS`` clause, so you may include it in your main code.


After that you can start using your models:

```python
# Create instance by save
book = Book(id=1, name='Mastering postmdel', tag="orm")
await book.save()

# Or by .create()
await Book.create(id=2, name='Learning Python', tag="python")

# Query

books = await Book.filter(tag="orm").all()
assert len(books) == 1
```



## Contributing

Please have a look at the `Contribution Guide <docs/CONTRIBUTING.md>`_


## License

This project is licensed under the MIT License - see the `LICENSE <LICENSE>`_ file for details