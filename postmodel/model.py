from copy import copy, deepcopy

from pypika import Query

from postmodel import fields
from postmodel.exceptions import ConfigurationError, OperationalError
from postmodel.fields import Field

class MetaInfo:
    __slots__ = (
        "abstract",
        "table",
        "app",
        "fields",
        "db_fields",
        "fields_db_projection",
        "fields_db_projection_reverse",
        "fields_map",
        "unique_together",
        "pk_attr",
        "generated_db_fields",
        "table_description",
        "pk",
        "db_pk_field",
    )

    def __init__(self, meta) -> None:
        self.abstract = getattr(meta, "abstract", False)  # type: bool
        self.table = getattr(meta, "table", "")  # type: str
        self.app = getattr(meta, "app", None)  # type: Optional[str]
        self.unique_together = self._get_unique_together(meta)  # type: Union[Tuple, List]
        self.fields = set()  # type: Set[str]
        self.db_fields = set()  # type: Set[str]
        self.fields_db_projection = {}  # type: Dict[str,str]
        self.fields_db_projection_reverse = {}  # type: Dict[str,str]
        self.fields_map = {}  # type: Dict[str, fields.Field]
        self.pk_attr = getattr(meta, "pk_attr", "")  # type: str
        self.generated_db_fields = None  # type: Tuple[str]  # type: ignore
        self.table_description = getattr(meta, "table_description", "")  # type: str
        self.pk = None  # type: fields.Field  # type: ignore
        self.db_pk_field = ""  # type: str

    def _get_unique_together(self, meta):
        unique_together = getattr(meta, "unique_together", None)

        if isinstance(unique_together, (list, tuple)):
            if unique_together and isinstance(unique_together[0], str):
                unique_together = (unique_together,)

        # return without validation, validation will be done further in the code
        return unique_together


    def finalise_pk(self) -> None:
        self.pk = self.fields_map[self.pk_attr]
        self.db_pk_field = self.pk.source_field or self.pk_attr

    def finalise_model(self) -> None:
        """
        Finalise the model after it had been fully loaded.
        """
        self.finalise_fields()

    def finalise_fields(self) -> None:
        self.db_fields = set(self.fields_db_projection.values())
        self.fields = set(self.fields_map.keys())
        self.fields_db_projection_reverse = {
            value: key for key, value in self.fields_db_projection.items()
        }

        generated_fields = []
        for field in self.fields_map.values():
            if not field.generated:
                continue
            generated_fields.append(field.source_field or field.model_field_name)
        self.generated_db_fields = tuple(generated_fields)  # type: ignore


class ModelMeta(type):
    __slots__ = ()

    def __new__(mcs, name: str, bases, attrs: dict, *args, **kwargs):
        fields_db_projection = {}  # type: Dict[str,str]
        fields_map = {}  # type: Dict[str, fields.Field]
        meta_class = attrs.get("Meta", type("Meta", (), {}))
        pk_attr = "id"

        meta = MetaInfo(meta_class)

        fields_map = {}
        fields_db_projection = {}

        for key, value in attrs.items():
            if isinstance(value, fields.Field):
                fields_map[key] = value
                value.model_field_name = key
                fields_db_projection[key] = value.source_field or key
        
        for key in fields_map.keys():
            attrs.pop(key)

        for base in bases:
            _meta = getattr(base, "_meta", None)
            if not _meta:
                continue
            fields_map.update(deepcopy(_meta.fields_map))

        meta.fields_map = fields_map
        meta.fields_db_projection = fields_db_projection
        meta.pk_attr = pk_attr
        if not fields_map:
            meta.abstract = True

        new_class = super().__new__(mcs, name, bases, attrs)  # type: "Model"  # type: ignore
        meta.finalise_fields()
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

    def __init__(self, *args, **kwargs) -> None:
        # self._meta is a very common attribute lookup, lets cache it.
        meta = self._meta
        self._saved_in_db = meta.pk_attr in kwargs and meta.pk.generated

        # Assign values and do type conversions
        passed_fields = {*kwargs.keys()}

        # Assign defaults for missing fields
        for key in meta.fields.difference(passed_fields):
            field_object = meta.fields_map[key]
            if callable(field_object.default):
                setattr(self, key, field_object.default())
            else:
                setattr(self, key, field_object.default)


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