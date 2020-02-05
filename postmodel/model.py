from copy import copy, deepcopy

from pypika import Query

from postmodel import fields
from postmodel.exceptions import ConfigurationError, OperationalError
from postmodel.fields import Field

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
        "unique_together",
        "indexes",
        "pk_attr",
        "table_description",
        "pk",
        "db_pk_field",
    )

    def __init__(self, meta) -> None:
        self.abstract = getattr(meta, "abstract", False)  # type: bool
        self.table = getattr(meta, "table", )  # type: str
        self.db_name = getattr(meta, "db_name", 'default')  # type: Optional[str]
        self.unique_together = self._get_together(meta, "unique_together")  # type: Union[Tuple, List]
        self.indexes = self._get_together(meta, "indexes")
        self.fields = set()  # type: Set[str]
        self.db_fields = set()  # type: Set[str]
        self.fields_db_projection = {}  # type: Dict[str,str]
        self.fields_db_projection_reverse = {}  # type: Dict[str,str]
        self.fields_map = {}  # type: Dict[str, fields.Field]
        self.pk_attr = getattr(meta, "pk_attr", "")  # type: str
        self.table_description = getattr(meta, "table_description", "")  # type: str
        self.pk = None  # type: fields.Field  # type: ignore
        self.db_pk_field = ""  # type: str

    def _get_together(self, meta, together: str):
        _together = getattr(meta, together, ())

        if isinstance(_together, (list, tuple)):
            if _together and isinstance(_together[0], str):
                _together = (_together,)

        # return without validation, validation will be done further in the code
        return _together

    def finalise_pk(self) -> None:
        self.pk = self.fields_map[self.pk_attr]
        self.db_pk_field = self.pk.source_field or self.pk_attr

    def finalise_model(self) -> None:
        """
        Finalise the model after it had been fully loaded.
        """
        if not self.abstract and not self.pk_attr:
            raise Exception('model must have pk or be abstract.')
        if self.pk_attr:
            self.finalise_pk()
        self.finalise_fields()

    def finalise_fields(self) -> None:
        self.db_fields = set(self.fields_db_projection.values())
        self.fields = set(self.fields_map.keys())
        self.fields_db_projection_reverse = {
            value: key for key, value in self.fields_db_projection.items()
        }


class ModelMeta(type):
    __slots__ = ()

    def __new__(mcs, name: str, bases, attrs: dict, *args, **kwargs):
        fields_db_projection = {}  # type: Dict[str,str]
        fields_map = {}  # type: Dict[str, fields.Field]
        meta_class = attrs.pop("Meta", type("Meta", (), {}))
        if not hasattr(meta_class, "table"):
            setattr(meta_class, "table", name)

        meta = MetaInfo(meta_class)

        fields_map = {}
        fields_db_projection = {}

        pk_attr = None

        for key, value in attrs.items():
            if isinstance(value, fields.Field):
                fields_map[key] = value
                value.model_field_name = key
                if value.pk:
                    if pk_attr != None:
                        raise Exception('duplicated pk not allowed.')
                    pk_attr = key
                fields_db_projection[key] = value.source_field or key
        
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
        return new_class


class Model(metaclass=ModelMeta):
    _meta = None
    _mapper_cache = {}

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

        # Assign defaults for missing fields
        for key in meta.fields.difference(passed_fields):
            field_object = meta.fields_map[key]
            if callable(field_object.default):
                setattr(self, key, field_object.default())
            else:
                setattr(self, key, field_object.default)
        
        self._snapshot_data = {}
    
    def make_snapshot(self):
        new_data = dict()
        for key in self._meta.fields_db_projection.keys():
            new_data[key] = deepcopy(getattr(self, key))
        self._snapshot_data = new_data
    
    @property
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
        if not self.pk:
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
    def pk(self):
        setattr(self, self._meta.pk_attr, value)
    
    @classmethod
    def get_mapper(cls):
        db_name = cls._meta.db_name
        if db_name in cls._mapper_cache:
            return cls._mapper_cache[db_name]
        else:
            mapper = Postmode.get_mapper(cls, db_name)
            cls._mapper_cache[db_name] = mapper
            return mapper