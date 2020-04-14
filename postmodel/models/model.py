from copy import copy, deepcopy

from postmodel.exceptions import ConfigurationError, OperationalError, StaleObjectError
from postmodel.main import Postmodel
from collections import OrderedDict
from .query import QuerySet, FilterBuilder
from .fields import Field, DataVersionField
import re
import datetime
import uuid
import json

_underscorer1 = re.compile(r'(.)([A-Z][a-z]+)')
_underscorer2 = re.compile('([a-z0-9])([A-Z])')

def camel_to_snake(s):
    """
    Is it ironic that this function is written in camel case, yet it
    converts to snake case? hmm..
    """
    subbed = _underscorer1.sub(r'\1_\2', s)
    return _underscorer2.sub(r'\1_\2', subbed).lower()

class MetaInfo:
    __slots__ = (
        "abstract",
        "table",
        "db_name",
        "fields",
        "db_fields",
        "fields_db_projection",
        "fields_db_projection_reverse",
        "fields_map",
        "auto_fields",
        "dataversion_field",
        "unique_together",
        "indexes",
        "pk_attr",
        "table_description",
        "pk",
        "db_pk_field",
        "filters"
    )

    def __init__(self, meta) -> None:
        self.abstract = getattr(meta, "abstract", False)  # type: bool
        self.table = getattr(meta, "table", )  # type: str
        self.db_name = getattr(meta, "db_name", 'default')  # type: Optional[str]
        self.unique_together = self._get_together(meta, "unique_together")  # type: Union[Tuple, List]
        self.indexes = self._get_together(meta, "indexes")
        self.fields = set()  # type: Set[str]
        self.db_fields = set()  # type: Set[str]
        self.fields_db_projection = OrderedDict()  # type: Dict[str,str]
        self.fields_db_projection_reverse = OrderedDict()  # type: Dict[str,str]
        self.fields_map = OrderedDict()  # type: Dict[str, fields.Field]
        self.auto_fields = [] # type List[fields.Field]
        self.dataversion_field = "" # type str
        self.pk_attr = getattr(meta, "pk_attr", "")  # type: str
        self.table_description = getattr(meta, "table_description", "")  # type: str
        self.pk = None  # type: fields.Field  # type: ignore
        self.db_pk_field = ""  # type: str
        self.filters = {}

    def _get_together(self, meta, together: str):
        _together = getattr(meta, together, ())

        if isinstance(_together, (list, tuple)):
            if _together and isinstance(_together[0], str):
                _together = (_together,)

        # return without validation, validation will be done further in the code
        return _together

    def finalise_pk(self) -> None:
        self.pk = self.fields_map[self.pk_attr]
        self.db_pk_field = self.pk.db_field or self.pk_attr

    def finalize_filters(self):
        for field_name, db_field in self.fields_db_projection.items():
            filters = FilterBuilder.get_filters_for_field(self.fields_map[field_name],
                    field_name, db_field)
            self.filters[field_name] = filters

    def get_filter(self, key: str) -> dict: # pragma: nocoverage
        return self.filters.get(key, None)

    def finalise_model(self) -> None:
        """
        Finalise the model after it had been fully loaded.
        """
        if not self.abstract and not self.pk_attr:
            raise Exception('model must have pk or be abstract.')
        if self.pk_attr:
            self.finalise_pk()
        self.finalize_filters()
        self.finalise_fields()

    def finalise_fields(self) -> None:
        self.db_fields = set(self.fields_db_projection.values())
        self.fields = set(self.fields_map.keys())
        self.fields_db_projection_reverse = {
            value: key for key, value in self.fields_db_projection.items()
        }
        for field_name in self.fields:
            field = self.fields_map[field_name]
            if hasattr(field, 'auto_value'):
                self.auto_fields.append(field)
            if isinstance(field, DataVersionField):
                if not self.dataversion_field:
                    self.dataversion_field = field_name
                else:
                    raise Exception('model class can only have one DataVersionField.')


class ModelMeta(type):
    __slots__ = ()

    def __new__(mcs, name: str, bases, attrs: dict, *args, **kwargs):
        fields_db_projection = OrderedDict()  # type: Dict[str,str]
        fields_map = OrderedDict()  # type: Dict[str, fields.Field]
        meta_class = attrs.pop("Meta", type("Meta", (), {}))
        if not hasattr(meta_class, "table"):
            setattr(meta_class, "table", camel_to_snake(name))

        meta = MetaInfo(meta_class)

        fields_map = {}
        fields_db_projection = {}

        pk_attr = None

        for key, value in attrs.items():
            if isinstance(value, Field):
                fields_map[key] = value
                value.model_field_name = key
                if value.pk:
                    if pk_attr != None:
                        raise Exception('duplicated pk not allowed.')
                    pk_attr = key
                fields_db_projection[key] = value.db_field or key

        for key in fields_map.keys():
            attrs.pop(key)

        for base in bases:
            _meta = getattr(base, "_meta", None)
            if not _meta:
                continue
            fields_map.update(deepcopy(_meta.fields_map))
            fields_db_projection.update(deepcopy(_meta.fields_db_projection))
            if _meta.pk_attr:
                if pk_attr != None:
                    raise Exception('duplicated pk not allowed.')
                else:
                    pk_attr = _meta.pk_attr

        meta.fields_map = fields_map
        meta.fields_db_projection = fields_db_projection
        if not fields_map:
            meta.abstract = True
        meta.pk_attr = pk_attr or ""

        attrs["_meta"] = meta
        new_class = super().__new__(mcs, name, bases, attrs)  # type: "Model"  # type: ignore
        meta.finalise_model()
        new_class.check()
        return new_class


class Model(metaclass=ModelMeta):
    _meta = None

    class Meta:
        """
        The ``Meta`` class is used to configure metadate for the Model.

        Usage:

        .. code-block:: python3

            class Foo(Model):
                ...

                class Meta:
                    table="custom_table"
                    unique_together=(("field_a", "field_b"), )
        """
        pass

    def __init__(self, *args, load_from_db=False, **kwargs) -> None:
        # self._meta is a very common attribute lookup, lets cache it.
        meta = self._meta
        self._saved_in_db = load_from_db

        # Assign values and do type conversions
        passed_fields = {*kwargs.keys()}

        for key, value in kwargs.items():
            if key in meta.fields_db_projection:
                field = meta.fields_map[key]
                if value is None and not field.null:
                    raise ValueError(f"{key} is non nullable field, but null was passed")
                setattr(self, key, field.to_python_value(value))

        # Assign defaults for missing fields
        for key in meta.fields.difference(passed_fields):
            field = meta.fields_map[key]
            if callable(field.default):
                setattr(self, key, field.default())
            else:
                setattr(self, key, field.default)

        self._snapshot_data = {}

        if self._saved_in_db:
            self.make_snapshot()

    @classmethod
    def _init_from_db(cls, **kwargs):
        instance = cls(load_from_db=True, **kwargs)
        return instance

    def make_snapshot(self):
        new_data = dict()
        for key in self._meta.fields_db_projection.keys():
            new_data[key] = deepcopy(getattr(self, key))
        self._snapshot_data = new_data

    def to_dict(self):
        data = dict()
        for key in self._meta.fields_db_projection.keys():
            data[key] = deepcopy(getattr(self, key))
        return data

    def to_jsondict(self):
        json_data = dict()
        for key in self._meta.fields_db_projection.keys():
            value = deepcopy(getattr(self, key))
            if isinstance(value, (datetime.date, datetime.datetime)):
                json_data[key] = value.isoformat()
            elif isinstance(value, uuid.UUID):
                json_data[key] = str(value)
            else:
                json_data[key] = value
        return json_data

    def to_json(self):
        return json.dumps(self.to_jsondict())

    def changed(self):
        now_data = dict()
        for key in self._meta.fields_db_projection.keys():
            now_data[key] = getattr(self, key)
        diff = self.dict_diff(now_data, self._snapshot_data)
        return diff.keys()

    def dict_diff(self, first, second):
        """ Return a dict of keys that differ with another config object.  If a value is
            not found in one fo the configs, it will be represented by None.
            @param first:   Fist dictionary to diff.
            @param second:  Second dicationary to diff.
            @return diff:   Dict of Key => (first.val, second.val)
        """
        diff = {}
        # Check all keys in first dict
        for key in first.keys():
            if key not in second:
                diff[key] = (first[key], None)
            elif (first[key] != second[key]):
                diff[key] = (first[key], second[key])
        return diff

    def __str__(self) -> str:
        return "<{}>".format(self.__class__.__name__)

    def __repr__(self) -> str:
        if self.pk:
            return "<{}: {}>".format(self.__class__.__name__, self.pk)
        return "<{}>".format(self.__class__.__name__)

    def __hash__(self) -> int:
        if not self.pk: # pragma: nocoverage
            raise TypeError("Model instances without id are unhashable")
        return hash(self.pk)

    def __eq__(self, other) -> bool:
        # pylint: disable=C0123
        if type(self) == type(other) and self.pk == other.pk:
            return True
        return False

    @property
    def pk(self):
        return getattr(self, self._meta.pk_attr)

    @pk.setter
    def pk(self, value):
        setattr(self, self._meta.pk_attr, value)

    def _auto_values(self):
        for field in self._meta.auto_fields:
            field.auto_value(self)

    @classmethod
    def first(cls):
        """
        Generates a QuerySet that returns the first record.
        """
        return QuerySet(cls).first()

    @classmethod
    def filter(cls, *args, **kwargs):
        """
        Generates a QuerySet with the filter applied.
        """
        return QuerySet(cls).filter(*args, **kwargs)

    @classmethod
    def exclude(cls, *args, **kwargs):
        """
        Generates a QuerySet with the exclude applied.
        """
        return QuerySet(cls).exclude(*args, **kwargs)

    @classmethod
    def all(cls):
        """
        Returns the complete QuerySet.
        """
        return QuerySet(cls)

    @classmethod
    def get(cls, *args, **kwargs):
        """
        Fetches a single record for a Model type using the provided filter parameters.

        .. code-block:: python3

            user = await User.get(username="foo")

        :raises MultipleObjectsReturned: If provided search returned more than one object.
        :raises DoesNotExist: If object can not be found.
        """
        return QuerySet(cls).get(*args, **kwargs)

    @classmethod
    def get_or_none(cls, *args, **kwargs):
        """
        Fetches a single record for a Model type using the provided filter parameters or None.

        .. code-block:: python3

            user = await User.get_or_none(username="foo")
        """
        return QuerySet(cls).filter(*args, **kwargs).first()


    async def save(self, update_fields = None, force=False) -> int:
        changed = self.changed()
        if len(changed) == 0:
            return

        condition_fields = []
        dataver_field_name = self._meta.dataversion_field
        if not force:
            self._auto_values()
            if dataver_field_name and dataver_field_name in self._snapshot_data:
                condition_fields.append((dataver_field_name, self._snapshot_data[dataver_field_name]))

        fileds = list(set(update_fields or ()) | set(self.changed()))

        mapper = self.get_mapper()
        if self._saved_in_db:
            ret = await mapper.update(self, update_fields=fileds, condition_fields=condition_fields)
            if ret == 0:
                raise StaleObjectError('model save failed.')
        else:
            ret = await mapper.insert(self)
            if ret == 0: # pragma: nocoverage
                raise StaleObjectError('model insert failed.')
            self._saved_in_db = True

        self.make_snapshot()

    async def delete(self) -> int:
        """
        Deletes the current model object.

        :raises OperationalError: If object has never been persisted.
        """
        if not self._saved_in_db:
            raise OperationalError("Can't delete unpersisted record")
        mapper = self.get_mapper()
        return await mapper.delete(self)

    @classmethod
    async def get_or_create(cls, defaults = None, **kwargs):
        """
        Fetches the object if exists (filtering on the provided parameters),
        else creates an instance with any unspecified parameters as default values.
        """
        if not defaults:
            defaults = {}
        instance = await cls.filter(**kwargs).first()
        if instance:
            return instance, False
        return await cls.create(**defaults, **kwargs), True


    @classmethod
    async def create(cls, **kwargs):
        """
        Create a record in the DB and returns the object.

        .. code-block:: python3

            user = await User.create(name="...", email="...")

        Equivalent to:

        .. code-block:: python3

            user = User(name="...", email="...")
            await user.save()
        """
        instance = cls(**kwargs)
        instance._auto_values()
        mapper = cls.get_mapper()
        await mapper.insert(instance)
        instance._saved_in_db = True
        instance.make_snapshot()
        return instance

    @classmethod
    async def bulk_create(cls, objects) -> None:
        """
        Bulk insert operation:

        .. note::
            The bulk insert operation will do the minimum to ensure that the object
            created in the DB has all the defaults and generated fields set,
            but may be incomplete reference in Python.

            e.g. ``IntField`` primary keys will not be populated.

        This is recommend only for throw away inserts where you want to ensure optimal
        insert performance.

        .. code-block:: python3

            User.bulk_create([
                User(name="...", email="..."),
                User(name="...", email="...")
            ])

        :param objects: List of objects to bulk create
        """
        mapper = cls.get_mapper()
        for obj in objects:
            obj._auto_values()
        await mapper.bulk_insert(objects)  # type: ignore
        for obj in objects:
            obj.make_snapshot()

    @classmethod
    def get_mapper(cls, using_db=None):
        db_name = using_db or cls._meta.db_name
        mapper = Postmodel.get_mapper(cls, db_name)
        return mapper

    @classmethod
    def check(cls) -> None:
        """
        Calls various checks to validate the model.

        :raises ConfigurationError: If the model has not been configured correctly.
        """
        cls._check_together("unique_together")
        cls._check_together("indexes")

    @classmethod
    def _check_together(cls, together: str) -> None:
        """Check the value of "unique_together" option."""
        _together = getattr(cls._meta, together)
        if not isinstance(_together, (tuple, list)):
            raise ConfigurationError(f"'{cls.__name__}.{together}' must be a list or tuple.")

        if any(not isinstance(unique_fields, (tuple, list)) for unique_fields in _together):
            raise ConfigurationError(
                f"All '{cls.__name__}.{together}' elements must be lists or tuples."
            )

        for fields_tuple in _together:
            for field_name in fields_tuple:
                field = cls._meta.fields_map.get(field_name)

                if not field:
                    raise ConfigurationError(
                        f"'{cls.__name__}.{together}' has no '{field_name}' field."
                    )