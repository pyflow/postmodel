
from typing import Any

from pypika import functions
from pypika.terms import AggregateFunction
from pypika.terms import Function as BaseFunction


class Function:
    database_func = BaseFunction

    def __init__(self, field_name, *args, **kwargs) -> None:
        self.field_name = field_name


class Trim(Function):
    database_func = functions.Trim


class Length(Function):
    database_func = functions.Length


class Coalesce(Function):
    database_func = functions.Coalesce


class Lower(Function):
    database_func = functions.Lower


class Upper(Function):
    database_func = functions.Upper



class Aggregate(Function):
    database_func = AggregateFunction


class Count(Aggregate):
    database_func = functions.Count


class Sum(Aggregate):
    database_func = functions.Sum


class Max(Aggregate):
    database_func = functions.Max


class Min(Aggregate):
    database_func = functions.Min


class Avg(Aggregate):
    database_func = functions.Avg
