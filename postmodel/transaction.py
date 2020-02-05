
from functools import wraps
from typing import Callable, Optional
from postmodel.main import Postmodel


def in_transaction(db_name: Optional[str] = None):
    """
    Transaction context manager.

    You can run your code inside ``async with in_transaction():`` statement to run it
    into one transaction. If error occurs transaction will rollback.

    """
    db = Postmodel.get_database(db_name)
    return db.in_transaction()


def atomic(db_name: Optional[str] = None):
    """
    Transaction decorator.

    You can wrap your function with this decorator to run it into one transaction.
    If error occurs transaction will rollback.

    """

    def wrapper(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            db = Postmodel.get_database(db_name)
            async with db.in_transaction():
                return await func(*args, **kwargs)

        return wrapped

    return wrapper
