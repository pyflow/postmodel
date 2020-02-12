
from typing import Any

class Function:
    is_aggregate = False
    is_analytics = False

    def __init__(self, field_name, *args, **kwargs) -> None:
        self.field_name = field_name
        self.args = args

class Trim(Function):
    pass


class Length(Function):
    pass


class Coalesce(Function):
    pass


class Lower(Function):
    pass


class Upper(Function):
    pass



class Aggregate(Function):
    is_aggregate = True
    pass


class Count(Aggregate):
    pass


class Sum(Aggregate):
    pass


class Max(Aggregate):
    pass


class Min(Aggregate):
    pass


class Avg(Aggregate):
    pass
