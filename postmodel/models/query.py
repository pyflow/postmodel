
from copy import copy
from typing import Any, Dict, List, Optional, Tuple

from pypika import Table
from pypika.terms import Criterion

from postmodel.exceptions import FieldError, OperationalError

import operator
from functools import partial

from pypika import Table, functions
from pypika.enums import SqlTypes

from postmodel.models.fields import Field


class FilterBuilder:
    @staticmethod
    def list_encoder(values, instance, field: Field):
        """Encodes an iterable of a given field into a database-compatible format."""
        return [field.to_db_value(element, instance) for element in values]

    @staticmethod
    def related_list_encoder(values, instance, field: Field):
        return [
            field.to_db_value(element.pk if hasattr(element, "pk") else element, instance)
            for element in values
        ]

    @staticmethod
    def bool_encoder(value, *args):
        return bool(value)

    @staticmethod
    def string_encoder(value, *args):
        return str(value)

    @staticmethod
    def is_in(field, value):
        return field.isin(value)

    @staticmethod
    def not_in(field, value):
        return field.notin(value) | field.isnull()

    @staticmethod
    def not_equal(field, value):
        return field.ne(value) | field.isnull()

    @staticmethod
    def is_null(field, value):
        if value:
            return field.isnull()
        return field.notnull()

    @staticmethod
    def not_null(field, value):
        if value:
            return field.notnull()
        return field.isnull()

    @staticmethod
    def contains(field, value):
        return functions.Cast(field, SqlTypes.VARCHAR).like(f"%{value}%")

    @staticmethod
    def starts_with(field, value):
        return functions.Cast(field, SqlTypes.VARCHAR).like(f"{value}%")

    @staticmethod
    def ends_with(field, value):
        return functions.Cast(field, SqlTypes.VARCHAR).like(f"%{value}")

    @staticmethod
    def insensitive_exact(field, value):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).eq(functions.Upper(f"{value}"))

    @staticmethod
    def insensitive_contains(field, value):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
            functions.Upper(f"%{value}%")
        )

    @staticmethod
    def insensitive_starts_with(field, value):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
            functions.Upper(f"{value}%")
        )

    @staticmethod
    def insensitive_ends_with(field, value):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
            functions.Upper(f"%{value}")
        )

    @staticmethod
    def get_filters_for_field(
        field_name: str, field: Optional[Field], source_field: str
    ) -> Dict[str, dict]:
        actual_field_name = field_name
        if field_name == "pk" and field:
            actual_field_name = field.model_field_name
        return {
            field_name: {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": operator.eq,
            },
            f"{field_name}__not": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.not_equal,
            },
            f"{field_name}__in": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.is_in,
                "value_encoder": FilterBuilder.list_encoder,
            },
            f"{field_name}__not_in": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.not_in,
                "value_encoder": FilterBuilder.list_encoder,
            },
            f"{field_name}__isnull": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.is_null,
                "value_encoder": FilterBuilder.bool_encoder,
            },
            f"{field_name}__not_isnull": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.not_null,
                "value_encoder": FilterBuilder.bool_encoder,
            },
            f"{field_name}__gte": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": operator.ge,
            },
            f"{field_name}__lte": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": operator.le,
            },
            f"{field_name}__gt": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": operator.gt,
            },
            f"{field_name}__lt": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": operator.lt,
            },
            f"{field_name}__contains": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.contains,
                "value_encoder": FilterBuilder.string_encoder,
            },
            f"{field_name}__startswith": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.starts_with,
                "value_encoder": FilterBuilder.string_encoder,
            },
            f"{field_name}__endswith": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.ends_with,
                "value_encoder": FilterBuilder.string_encoder,
            },
            f"{field_name}__iexact": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.insensitive_exact,
                "value_encoder": FilterBuilder.string_encoder,
            },
            f"{field_name}__icontains": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.insensitive_contains,
                "value_encoder": FilterBuilder.string_encoder,
            },
            f"{field_name}__istartswith": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.insensitive_starts_with,
                "value_encoder": FilterBuilder.string_encoder,
            },
            f"{field_name}__iendswith": {
                "field": actual_field_name,
                "source_field": source_field,
                "operator": FilterBuilder.insensitive_ends_with,
                "value_encoder": FilterBuilder.string_encoder,
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
            raise OperationalError("All ordered arguments must be Q nodes")
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
            raise OperationalError("AND operation requires a Q node")
        return QueryExpression(self, other, join_type=self.AND)

    def __or__(self, other) -> "QueryExpression":
        if not isinstance(other, QueryExpression):
            raise OperationalError("OR operation requires a Q node")
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
    
    def get(self, *args, **kwargs):
        return self
    
    async def __await__(self):
        return self._execute().__await__()

    async def __aiter__(self):
        for val in await self:
            yield val

    async def _execute(self):
        raise NotImplementedError()