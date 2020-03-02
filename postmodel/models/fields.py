import datetime
import functools
import json
import uuid
from decimal import Decimal
from enum import Enum, IntEnum
from typing import TYPE_CHECKING, Any, Optional
from typing import Any, Optional, Type, TypeVar, Union
from uuid import UUID

import ciso8601

from postmodel.exceptions import ConfigurationError, NoValuesFetched, OperationalError

# Doing this we can replace json dumps/loads with different implementations
JSON_DUMPS = functools.partial(json.dumps, separators=(",", ":"))
JSON_LOADS = json.loads


class Field:
    """
    Base Field type.
    """

    __slots__ = (
        "type",
        "db_field",
        "pk",
        "default",
        "null",
        "unique",
        "index",
        "model_field_name",
        "reference",
        "description",
    )

    has_db_field = True
    indexable: bool = True


    def __init__(
        self,
        type=None,  # pylint: disable=W0622
        db_field: Optional[str] = None,
        pk: bool = False,
        null: bool = False,
        default: Any = None,
        unique: bool = False,
        index: bool = False,
        reference: Optional[str] = None,
        description: Optional[str] = None,
        **kwargs
    ) -> None:
        self.type = type
        self.db_field = db_field
        self.pk = pk
        self.default = default
        self.null = null
        self.unique = unique
        self.index = index
        self.model_field_name = ""  # type: str
        self.reference = reference
        self.description = description

    def to_db_value(self, value: Any) -> Any:
        if value is None or type(value) == self.type:  # pylint: disable=C0123
            return value
        return self.type(value)

    def to_python_value(self, value: Any) -> Any:
        if value is None or isinstance(value, self.type):
            return value
        return self.type(value)

    @property
    def required(self):
        return self.default is None and not self.null


class IntField(Field):
    """
    Integer field. (32-bit signed)

    ``pk`` (bool):
        True if field is Primary Key.
    """

    def __init__(self, pk: bool = False, **kwargs) -> None:
        super().__init__(int, pk=pk, **kwargs)


class BigIntField(Field):
    """
    Big integer field. (64-bit signed)

    ``pk`` (bool):
        True if field is Primary Key.
    """

    def __init__(self, pk: bool = False, **kwargs) -> None:
        super().__init__(int, pk=pk, **kwargs)


class AutoField(BigIntField):
    def __init__(self, **kwargs) -> None:
        kwargs.pop('pk', None)
        super(AutoField, self).__init__(pk=True, **kwargs)


class DataVersionField(BigIntField):
    def __init__(self, **kwargs) -> None:
        kwargs.pop('pk', None)
        default = kwargs.pop('default', 0)
        super(DataVersionField, self).__init__(pk=False, default=default, **kwargs)

    def auto_value(self, model_instance):
        current_value = getattr(model_instance, self.model_field_name)
        setattr(model_instance, self.model_field_name, current_value+1)


class SmallIntField(Field):
    """
    Small integer field. (16-bit signed)
    """

    __slots__ = ()

    def __init__(self, **kwargs) -> None:
        super().__init__(int, **kwargs)


class CharField(Field):
    """
    Character field.

    You must provide the following:

    ``max_length`` (int):
        Maximum length of the field in characters.
    """

    __slots__ = ("max_length",)

    def __init__(self, max_length: int, **kwargs) -> None:
        if int(max_length) < 1:
            raise ConfigurationError("'max_length' must be >= 1")
        self.max_length = int(max_length)
        super().__init__(str, **kwargs)


class TextField(Field):
    """
    Large Text field.
    """

    __slots__ = ()

    def __init__(self, **kwargs) -> None:
        super().__init__(str, **kwargs)


class BooleanField(Field):
    """
    Boolean field.
    """

    __slots__ = ()

    def __init__(self, **kwargs) -> None:
        super().__init__(bool, **kwargs)


class DecimalField(Field):
    """
    Accurate decimal field.

    You must provide the following:

    ``max_digits`` (int):
        Max digits of significance of the decimal field.
    ``decimal_places`` (int):
        How many of those signifigant digits is after the decimal point.
    """

    __slots__ = ("max_digits", "decimal_places")

    def __init__(self, max_digits: int, decimal_places: int, **kwargs) -> None:
        if int(max_digits) < 1:
            raise ConfigurationError("'max_digits' must be >= 1")
        if int(decimal_places) < 0:
            raise ConfigurationError("'decimal_places' must be >= 0")
        super().__init__(Decimal, **kwargs)
        self.max_digits = max_digits
        self.decimal_places = decimal_places


class DatetimeField(Field):
    """
    Datetime field.

    ``auto_now`` and ``auto_now_add`` is exclusive.
    You can opt to set neither or only ONE of them.

    ``auto_now`` (bool):
        Always set to ``datetime.utcnow()`` on save.
    ``auto_now_add`` (bool):
        Set to ``datetime.utcnow()`` on first save only.
    """

    __slots__ = ("auto_now", "auto_now_add")

    def __init__(self, auto_now: bool = False, auto_now_add: bool = False, **kwargs) -> None:
        if auto_now_add and auto_now:
            raise ConfigurationError("You can choose only 'auto_now' or 'auto_now_add'")
        super().__init__(datetime.datetime, **kwargs)
        self.auto_now = auto_now
        self.auto_now_add = auto_now | auto_now_add

    def to_python_value(self, value: Any) -> Optional[datetime.datetime]:
        if value is None or isinstance(value, datetime.datetime):
            return value
        return ciso8601.parse_datetime(value)

    def auto_value(self, model_instance):
        current_value = getattr(model_instance, self.model_field_name)
        if self.auto_now or (self.auto_now_add and current_value is None):
            value = datetime.datetime.utcnow()
            setattr(model_instance, self.model_field_name, value)


class DateField(Field):
    """
    Date field.
    """

    __slots__ = ()

    def __init__(self, **kwargs) -> None:
        super().__init__(datetime.date, **kwargs)

    def to_python_value(self, value: Any) -> Optional[datetime.date]:
        if value is None or isinstance(value, datetime.date):
            return value
        return ciso8601.parse_datetime(value).date()


class TimeDeltaField(Field):
    """
    A field for storing time differences.
    """

    __slots__ = ()

    def __init__(self, **kwargs) -> None:
        super().__init__(datetime.timedelta, **kwargs)

    def to_python_value(self, value: Any) -> Optional[datetime.timedelta]:
        if value is None or isinstance(value, datetime.timedelta):
            return value
        return datetime.timedelta(microseconds=value)

    def to_db_value(self, value: Optional[datetime.timedelta]) -> Optional[int]:
        if value is None:
            return None
        return (value.days * 86400000000) + (value.seconds * 1000000) + value.microseconds


class FloatField(Field):
    """
    Float (double) field.
    """

    __slots__ = ()

    def __init__(self, **kwargs) -> None:
        super().__init__(float, **kwargs)


class JSONField(Field):
    """
    JSON field.

    This field can store dictionaries or lists of any JSON-compliant structure.

    ``encoder``:
        The JSON encoder. The default is recommended.
    ``decoder``:
        The JSON decoder. The default is recommended.
    """

    __slots__ = ("encoder", "decoder")

    def __init__(self, encoder=JSON_DUMPS, decoder=JSON_LOADS, **kwargs) -> None:
        super().__init__(type=(dict, list), **kwargs)
        self.encoder = encoder
        self.decoder = decoder

    def to_db_value(self, value: Optional[Union[dict, list]]) -> Optional[str]:
        if value is None:
            return None
        return self.encoder(value)

    def to_python_value(
        self, value: Optional[Union[str, dict, list]]
    ) -> Optional[Union[dict, list]]:
        if value is None or isinstance(value, self.type):
            return value
        return self.decoder(value)


class UUIDField(Field):
    """
    UUID Field

    This field can store uuid value.

    If used as a primary key, it will auto-generate a UUID4 by default.
    """

    def __init__(self, **kwargs) -> None:
        if kwargs.get("pk", False):
            if "default" not in kwargs:
                kwargs["default"] = uuid.uuid4
        super().__init__(type=UUID, **kwargs)

    def to_db_value(self, value: Any) -> Optional[str]:
        if value is None:
            return None
        return str(value)

    def to_python_value(self, value: Any) -> Optional[uuid.UUID]:
        if value is None or isinstance(value, self.type):
            return value
        return uuid.UUID(value)

class BinaryField(Field):  # type: ignore
    """
    Binary field.

    This is for storing ``bytes`` objects.
    Note that filter or queryset-update operations are not supported.
    """

    indexable = False

    def __init__(self, **kwargs) -> None:
        super().__init__(str, **kwargs)
