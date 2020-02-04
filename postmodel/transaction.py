
from functools import wraps
from typing import Callable, Optional

def _get_engine(engine_name):
    from tortoise import Postmodel

    return Postmodel.get_engine(engine_name)


def in_transaction(engine_name: Optional[str] = None) -> "TransactionContext":
    """
    Transaction context manager.

    You can run your code inside ``async with in_transaction():`` statement to run it
    into one transaction. If error occurs transaction will rollback.

    """
    engine = _get_engine(engine_name)
    return engine.in_transaction()


def atomic(engine_name: Optional[str] = None) -> Callable:
    """
    Transaction decorator.

    You can wrap your function with this decorator to run it into one transaction.
    If error occurs transaction will rollback.

    """

    def wrapper(func):
        @wraps(func)
        async def wrapped(*args, **kwargs):
            engine = _get_engine(engine_name)
            async with engine.in_transaction():
                return await func(*args, **kwargs)

        return wrapped

    return wrapper
