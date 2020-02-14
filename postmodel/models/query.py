
from copy import copy
from typing import Any, Dict, List, Optional, Tuple
from enum import Enum

from postmodel.exceptions import FieldError, OperationalError
from .fields import Field
from functools import partial


class Order(Enum):
    asc = "ASC"
    desc = "DESC"

class FilterBuilder:
    @staticmethod
    def unary_encoder(value, **kwargs):
        return None

    @staticmethod
    def list_encoder(values, field, **kwargs):
        """Encodes an iterable of a given field into a database-compatible format."""
        return [field.to_db_value(element) for element in values]

    @staticmethod
    def string_contains_encoder(value, **kwargs):
        return f'%{value}%'

    @staticmethod
    def string_starts_encoder(value, **kwargs):
        return f'{value}%'

    @staticmethod
    def string_ends_encoder(value, **kwargs):
        return f'%{value}'

    @staticmethod
    def string_encoder(value, **kwargs):
        return str(value)

    @staticmethod
    def get_filters_for_field(field, field_name: str, db_field: str) -> Dict[str, dict]:
        actual_field_name = field_name
        return {
            field_name: {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": 'equal',
            },
            f"{field_name}__not": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": 'not_equal'
            },
            f"{field_name}__in": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": 'is_in',
                "value_encoder": partial(FilterBuilder.list_encoder, field=field)
            },
            f"{field_name}__not_in": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": 'not_in',
                "value_encoder": partial(FilterBuilder.list_encoder, field=field)
            },
            f"{field_name}__isnull": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": 'is_null',
                "value_encoder": FilterBuilder.unary_encoder,
            },
            f"{field_name}__not_isnull": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": 'not_null',
                "value_encoder": FilterBuilder.unary_encoder,
            },
            f"{field_name}__gte": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "greater_equal",
            },
            f"{field_name}__lte": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "less_equal",
            },
            f"{field_name}__gt": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "greater_than",
            },
            f"{field_name}__lt": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "less_than",
            },
            f"{field_name}__contains": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "contains",
                "value_encoder": FilterBuilder.string_contains_encoder,
            },
            f"{field_name}__startswith": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "starts_with",
                "value_encoder": FilterBuilder.string_starts_encoder,
            },
            f"{field_name}__endswith": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "ends_with",
                "value_encoder": FilterBuilder.string_ends_encoder,
            },
            f"{field_name}__iexact": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "insensitive_exact",
                "value_encoder": FilterBuilder.string_encoder,
            },
            f"{field_name}__icontains": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "insensitive_contains",
                "value_encoder": FilterBuilder.string_contains_encoder,
            },
            f"{field_name}__istartswith": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "insensitive_starts_with",
                "value_encoder": FilterBuilder.string_starts_encoder,
            },
            f"{field_name}__iendswith": {
                "field": actual_field_name,
                "field_type": field.type,
                "db_field": db_field,
                "operator": "insensitive_ends_with",
                "value_encoder": FilterBuilder.string_ends_encoder,
            },
        }


class QueryExpression:
    __slots__ = (
        "children",
        "filters",
        "join_type",
        "_is_negated",
        "_annotations",
        "_custom_filters",
    )

    AND = "AND"
    OR = "OR"

    def __init__(self, *args, join_type=AND, **kwargs) -> None:
        if args and kwargs:
            newarg = QueryExpression(join_type=join_type, **kwargs)
            args = (newarg,) + args
            kwargs = {}
        if not all(isinstance(node, QueryExpression) for node in args):
            raise OperationalError("All ordered arguments must be QueryExpression nodes")
        self.children: Tuple[QueryExpression, ...] = args
        self.filters: Dict[str, Any] = kwargs
        if join_type not in {self.AND, self.OR}:
            raise OperationalError("join_type must be AND or OR")
        self.join_type = join_type
        self._is_negated = False
        self._annotations: Dict[str, Any] = {}
        self._custom_filters: Dict[str, Dict[str, Any]] = {}

    def __and__(self, other) -> "QueryExpression":
        if not isinstance(other, QueryExpression):
            raise OperationalError("AND operation requires a QueryExpression node")
        return QueryExpression(self, other, join_type=self.AND)

    def __or__(self, other) -> "QueryExpression":
        if not isinstance(other, QueryExpression):
            raise OperationalError("OR operation requires a QueryExpression node")
        return QueryExpression(self, other, join_type=self.OR)

    def __invert__(self) -> "QueryExpression":
        q = QueryExpression(*self.children, join_type=self.join_type, **self.filters)
        q.negate()
        return q

    def negate(self) -> None:
        self._is_negated = not self._is_negated

Q = QueryExpression

class QuerySet:
    def __init__(self, model_class):
        self.model_class = model_class
        self.fields = model_class._meta.db_fields
        self.db_name = "default"

        self._expect_single = False
        self._return_single = False

        self._limit: Optional[int] = None
        self._offset: Optional[int] = None
        self._orderings: List[Tuple[str, Any]] = []
        self._expressions: List[QueryExpression] = []
        self._distinct: bool = False

    def _clone(self):
        return self

    def _filter(self, *args, **kwargs):
        queryset = self._clone()
        for arg in args:
            if not isinstance(arg, QueryExpression):
                raise TypeError("expected QueryExpression objects as args")
            queryset._expressions.append(arg)

        for key, value in kwargs.items():
            queryset._expressions.append(QueryExpression(**{key: value}))

        return queryset

    def filter(self, *args, **kwargs):
        """
        Filters QuerySet by given kwargs. You can filter by related objects like this:

        .. code-block:: python3

            Team.filter(events__tournament__name='Test')

        You can also pass QueryExpression objects to filters as args.
        """
        return self._filter(*args, **kwargs)

    def _exclude(self, *args, **kwargs):
        queryset = self._clone()
        for arg in args:
            if not isinstance(arg, QueryExpression):
                raise TypeError("expected QueryExpression objects as args")
            queryset._expressions.append(~arg)

        for key, value in kwargs.items():
            queryset._expressions.append(~QueryExpression(**{key: value}))

        return queryset

    def exclude(self, *args, **kwargs):
        """
        Same as .filter(), but with appends all args with NOT
        """
        return self._exclude(*args, **kwargs)

    def order_by(self, *orderings: str):
        """
        Accept args to filter by in format like this:

        .. code-block:: python3

            .order_by('name', '-tournament__name')

        Supports ordering by related models too.
        """
        queryset = self._clone()
        new_ordering = []
        for ordering in orderings:
            order_type = Order.asc
            if ordering[0] == "-":
                field_name = ordering[1:]
                order_type = Order.desc
            else:
                field_name = ordering

            if not (
                field_name.split("__")[0] in self.fields
            ):
                raise FieldError(f"Unknown field {field_name} for model {self.model_class.__name__}")
            new_ordering.append((field_name, order_type))
        queryset._orderings = new_ordering
        return queryset

    def limit(self, limit: int):
        """
        Limits QuerySet to given length.
        """
        queryset = self._clone()
        queryset._limit = limit
        return queryset

    def offset(self, offset: int):
        """
        Query offset for QuerySet.
        """
        queryset = self._clone()
        queryset._offset = offset
        return queryset

    def distinct(self):
        """
        Make QuerySet distinct.

        Only makes sense in combination with a ``.values()`` or ``.values_list()`` as it
        precedes all the fetched fields with a distinct.
        """
        queryset = self._clone()
        queryset._distinct = True
        return queryset

    def delete(self):
        return DeleteQuery(
            model_class=self.model_class,
            db_name = self.db_name,
            expressions = self._expressions
        )

    def update(self, **kwargs):
        return UpdateQuery(
            model_class=self.model_class,
            db_name = self.db_name,
            expressions = self._expressions,
            update_kwargs=kwargs
        )

    def count(self):
        return CountQuery(
            model_class=self.model_class,
            db_name = self.db_name,
            expressions = self._expressions,
            limit=self._limit,
            offset=self._offset,
        )

    def all(self):
        """
        Return the whole QuerySet.
        Essentially a no-op except as the only operation.
        """
        return self._clone()

    def first(self):
        """
        Limit queryset to one object and return one object instead of list.
        """
        queryset = self._clone()
        queryset._limit = 1
        queryset._return_single = True
        return queryset  # type: ignore

    def get(self, *args, **kwargs):
        """
        Fetch exactly one object matching the parameters.
        """
        queryset = self.filter(*args, **kwargs)
        queryset._limit = 2
        queryset._expect_single = True
        return queryset  # type: ignore

    def get_or_none(self, *args, **kwargs):
        """
        Fetch exactly one object matching the parameters.
        """
        queryset = self.filter(*args, **kwargs)
        queryset._limit = 1
        queryset._return_single = True
        return queryset  # type: ignore

    async def explain(self) -> Any:
        """Fetch and return information about the query execution plan.

        This is done by executing an ``EXPLAIN`` query whose exact prefix depends
        on the database backend, as documented below.

        - PostgreSQL: ``EXPLAIN (FORMAT JSON, VERBOSE) ...``

        .. note::
            This is only meant to be used in an interactive environment for debugging
            and query optimization.
            **The output format may (and will) vary greatly depending on the database backend.**
        """
        mapper = self.model_class.get_mapper(self.db_name)
        return await mapper.explain(self)

    def using_db(self, db_name):
        """
        Executes query in provided db client.
        Useful for transactions workaround.
        """
        queryset = self._clone()
        queryset.db_name = db_name
        return queryset

    def __await__(self):
        return self._execute().__await__()

    async def __aiter__(self):
        for val in await self:
            yield val

    async def _execute(self):
        mapper = self.model_class.get_mapper(self.db_name)
        return await mapper.query(self)


class UpdateQuery:
    __slots__ = ("model_class", "db_name", "expressions", "update_kwargs")

    def __init__(self, model_class, db_name, expressions, update_kwargs) -> None:
        self.model_class = model_class
        self.db_name = db_name
        self.update_kwargs = update_kwargs
        self.expressions = expressions

    def __await__(self):
        return self._execute().__await__()

    async def _execute(self) -> int:
        mapper = self.model_class.get_mapper(self.db_name)
        return await mapper.query_update(self)


class DeleteQuery:
    __slots__ = ("model_class", "db_name", "expressions")

    def __init__(self, model_class, db_name, expressions) -> None:
        self.model_class = model_class
        self.db_name = db_name
        self.expressions = expressions

    def __await__(self):
        return self._execute().__await__()

    async def _execute(self) -> int:
        mapper = self.model_class.get_mapper(self.db_name)
        return await mapper.query_delete(self)


class CountQuery:
    __slots__ = ("model_class", "db_name", "expressions", "limit", "offset")

    def __init__(self, model_class, db_name, expressions, limit, offset) -> None:
        self.model_class = model_class
        self.db_name = db_name
        self.expressions = expressions
        self.limit = limit
        self.offset = offset


    def __await__(self):
        return self._execute().__await__()

    async def _execute(self) -> int:
        mapper = self.model_class.get_mapper(self.db_name)
        count = await mapper.query_count(self)
        return count
