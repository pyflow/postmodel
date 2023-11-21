
from hashlib import sha256
from typing import List, Set, Any, Iterable, Union, Optional
import operator
from pypika import functions
from pypika import Parameter
from pypika.enums import SqlTypes, SqlType, JSONOperators, Dialects
from pypika.terms import Criterion, BasicCriterion, Array, Tuple, Field, Function
from pypika.utils import format_alias_sql
from functools import partial
from copy import deepcopy
from postmodel.models.functions import Function
import json

def parameter(index: int) -> Parameter:
    return Parameter("$%d" % (index + 1,))

class BaseTableSchemaGenerator:
    DIALECT = "sql"
    TABLE_CREATE_TEMPLATE = 'CREATE TABLE {exists}"{table_name}" ({fields}){extra}{comment};'
    TABLE_DELETE_TEMPLATE = 'DROP TABLE "{table_name}" CASCADE;'
    TABLE_RENAME_TEMPLATE = 'ALTER TABLE "{table_name}" RENAME TO "{new_table_name}";'

    FIELD_TEMPLATE = '"{name}" {type} {nullable} {unique}{primary}{comment}'
    PRIMARY_KEY_TEMPLATE = 'PRIMARY KEY ({primary_keys})'
    INDEX_CREATE_TEMPLATE = 'CREATE INDEX {exists}"{index_name}" ON "{table_name}" ({fields});'
    UNIQUE_CONSTRAINT_CREATE_TEMPLATE = 'CONSTRAINT "{index_name}" UNIQUE ({fields})'

    FIELD_TYPE_MAP = {
        'IntField': 'INT',
        'BigIntField': 'BIGINT',
        'AutoField': 'BIGSERIAL',
        'DataVersionField': 'BIGINT',
        'SmallIntField': 'SMALLINT',
        'CharField': lambda x: f"VARCHAR({x.max_length})",
        'TextField': 'TEXT',
        'BooleanField': 'BOOL',
        'DecimalField': lambda x: f"DECIMAL({x.max_digits},{x.decimal_places})",
        'DatetimeField': 'TIMESTAMP',
        'DateField': 'DATE',
        'TimeDeltaField': 'BIGINT',
        'FloatField': 'DOUBLE PRECISION',
        'JSONField': 'JSONB',
        'UUIDField': 'UUID',
        'BinaryField': "BYTEA"
    }
    def __init__(self, meta_info) -> None:
        self.meta_info = meta_info

    def quote(self, val: str) -> str:
        return f'"{val}"'

    @staticmethod
    def _make_hash(*args: str, length: int) -> str:
        # Hash a set of string values and get a digest of the given length.
        return sha256(";".join(args).encode("utf-8")).hexdigest()[:length]

    def _generate_index_name(self, prefix, field_names: List[str]) -> str:
        # NOTE: for compatibility, index name should not be longer than 30
        # characters (Oracle limit).
        # That's why we slice some of the strings here.
        table_name = self.meta_info.table
        index_name = "{}_{}_{}_{}".format(
            prefix,
            table_name[:11],
            field_names[0][:7],
            self._make_hash(table_name, *field_names, length=6),
        )
        return index_name


    def get_create_schema_sql(self, safe=True) -> str:
        exists="IF NOT EXISTS " if safe else ""
        meta = self.meta_info
        table_name = meta.table
        schema_sql = []

        fields_with_index = []
        fields_sql = []
        for name, field in meta.fields_map.items():
            db_field = meta.fields_db_projection[name]
            nullable = "NOT NULL" if not field.null else ""
            unique = "UNIQUE" if field.unique else ""
            is_pk = field.pk or name == meta.primary_key
            field_type = self.FIELD_TYPE_MAP[type(field).__name__]
            if callable(field_type):
                field_type = field_type(field)
            if field.index and not field.pk:
                fields_with_index.append(field)
            sql = self.FIELD_TEMPLATE.format(
                name=db_field,
                type=field_type,
                nullable=nullable,
                unique="" if is_pk else unique,
                comment= "",
                primary=" PRIMARY KEY" if is_pk else "",
            ).strip()
            fields_sql.append(sql)

        db_pk_field = meta.db_pk_field
        if isinstance(db_pk_field, tuple):
            sql = self.PRIMARY_KEY_TEMPLATE.format(primary_keys=', '.join(db_pk_field))
            fields_sql.append(sql)

        for unique_together_list in meta.unique_together:
            field_names = unique_together_list
            sql = self.UNIQUE_CONSTRAINT_CREATE_TEMPLATE.format(
                index_name=self._generate_index_name("uniq",  field_names),
                fields=", ".join([self.quote(f) for f in field_names]),
            )
            fields_sql.append(sql)

        table_create_sql = self.TABLE_CREATE_TEMPLATE.format(
            exists = exists,
            table_name = table_name,
            fields = "\n    {}\n".format(",\n    ".join(fields_sql)),
            extra = "",
            comment = ""
        )
        schema_sql.append(table_create_sql)

        for field in fields_with_index:
            field_names = [meta.fields_db_projection[field.model_field_name]]
            sql = self.INDEX_CREATE_TEMPLATE.format(
                exists="IF NOT EXISTS " if safe else "",
                index_name=self._generate_index_name("idx", field_names),
                table_name=table_name,
                fields=", ".join([self.quote(f) for f in field_names]),
            )
            schema_sql.append(sql)

        for fields in meta.indexes:
            field_names = fields
            sql = self.INDEX_CREATE_TEMPLATE.format(
                exists="IF NOT EXISTS " if safe else "",
                index_name=self._generate_index_name("idx", field_names),
                table_name=table_name,
                fields=", ".join([self.quote(f) for f in field_names]),
            )
            schema_sql.append(sql)

        return '\n'.join(schema_sql)

class PostgreInCriterion(Criterion):
    value_type_map = {
        int: "bigint",
        str: "text"
    }
    def __init__(self, field_name, param, value_type):
        self.field_name = field_name
        self.param = param
        self.value_type = self.value_type_map[value_type]

    def get_sql(self, **kwargs):
        return f'"{self.field_name}" = ANY({self.param}::{self.value_type}[])'

class PostgreNotInCriterion(PostgreInCriterion):
    def get_sql(self, **kwargs):
        return f'"{self.field_name}" <> ALL({self.param}::{self.value_type}[])'



class ArrayCriterion(Tuple):
    def get_sql(self, **kwargs: Any) -> str:
        dialect = kwargs.get("dialect", None)
        values = ",".join(term.get_sql(**kwargs) for term in self.values)

        sql = "[{}]".format(values)
        if dialect in (Dialects.POSTGRESQL, Dialects.REDSHIFT):
            sql = "ARRAY[{}]".format(values) if len(values) > 0 else "'{}'"

        return format_alias_sql(sql, self.alias, **kwargs)


class JSONCriterion(BasicCriterion):
    @classmethod
    def create(cls, criterion):
        c = criterion
        return cls(c.comparator, c.left, c.right, c.alias)

    def get_json_value(self, key_or_index: Union[str, int]) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_JSON_VALUE, self, self.wrap_constant(key_or_index))

    def get_text_value(self, key_or_index: Union[str, int]) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_TEXT_VALUE, self, self.wrap_constant(key_or_index))

    def get_path_json_value(self, path_json: str) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_PATH_JSON_VALUE, self, self.wrap_json(path_json))

    def get_path_text_value(self, path_json: str) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_PATH_TEXT_VALUE, self, self.wrap_json(path_json))

    def has_key(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.HAS_KEY, self, self.wrap_json(other))

    def contains(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.CONTAINS, self, self.wrap_json(other))

    def contained_by(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.CONTAINED_BY, self, self.wrap_json(other))

    def has_keys(self, other: Iterable) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.HAS_KEYS, self, ArrayCriterion(*other))

    def has_any_keys(self, other: Iterable) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.HAS_ANY_KEYS, self, ArrayCriterion(*other))


class JSONField(Field):
    @classmethod
    def create(cls, field):
        f = field
        return cls(f.name, f.alias, f.table)

    def get_json_value(self, key_or_index: Union[str, int]) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_JSON_VALUE, self, self.wrap_constant(key_or_index))

    def get_text_value(self, key_or_index: Union[str, int]) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_TEXT_VALUE, self, self.wrap_constant(key_or_index))

    def get_path_json_value(self, path_json: str) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_PATH_JSON_VALUE, self, self.wrap_json(path_json))

    def get_path_text_value(self, path_json: str) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.GET_PATH_TEXT_VALUE, self, self.wrap_json(path_json))

    def has_key(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.HAS_KEY, self, self.wrap_json(other))

    def contains(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.CONTAINS, self, self.wrap_json(other))

    def contained_by(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.CONTAINED_BY, self, self.wrap_json(other))

    def has_keys(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.HAS_KEYS, self, ArrayCriterion(*other))

    def has_any_keys(self, other: Any) -> "JSONCriterion":
        return JSONCriterion(JSONOperators.HAS_ANY_KEYS, self, ArrayCriterion(*other))


def encode_json_value(value):
    if value == None:
        return None, None
    elif isinstance(value, bool):
        return SqlTypes.BOOLEAN, value
    elif isinstance(value, (int, float)):
        return SqlTypes.NUMERIC, value
    elif isinstance(value, str):
        return SqlTypes.VARCHAR, json.dumps(value)
    elif isinstance(value, (dict, list, tuple)):
        return SqlType("jsonb"), json.dumps(value)
    else:
        raise Exception(f'unsupported json value {value} to encode')


def get_json_field(table, field_name):
    names = field_name.split('.')
    name = names[0]
    attributes = names[1:]
    field = JSONField.create(getattr(table, name))
    for attr in attributes:
        field = field.get_json_value(attr)

    return field


class JSONFilterFunctions:
    @staticmethod
    def equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        value_type, new_value =  encode_json_value(value)
        if value_type:
            field = functions.Cast(field, value_type)
        return operator.eq(field, param_or_value), new_value

    @staticmethod
    def not_equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        value_type, new_value =  encode_json_value(value)
        if value_type:
            field = functions.Cast(field, value_type)
        return (field.ne(param_or_value) | field.isnull()), new_value

    @staticmethod
    def greater_equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        value_type, new_value =  encode_json_value(value)
        if value_type:
            field = functions.Cast(field, value_type)
        return operator.ge(field, param_or_value), new_value

    @staticmethod
    def greater_than(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        value_type, new_value =  encode_json_value(value)
        if value_type:
            field = functions.Cast(field, value_type)
        return operator.gt(field, param_or_value), new_value

    @staticmethod
    def less_equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        value_type, new_value =  encode_json_value(value)
        if value_type:
            field = functions.Cast(field, value_type)
        return operator.le(field, param_or_value), new_value

    @staticmethod
    def less_than(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        value_type, new_value =  encode_json_value(value)
        if value_type:
            field = functions.Cast(field, value_type)
        return operator.lt(field, param_or_value), new_value

    @staticmethod
    def has_key(field, param=None, value=None, **kwargs):
        return field.has_key(param), value

    @staticmethod
    def has_keys(field, param_index=None, value=None, **kwargs):
        params = [ parameter(i) for i in range(param_index, param_index + len(value)) ]
        value_type, new_value =  encode_json_value(value)
        return field.has_keys(params), value

    @staticmethod
    def has_anykeys(field, param_index=None, value=None, **kwargs):
        params = [ parameter(i) for i in range(param_index, param_index + len(value)) ]
        value_type, new_value =  encode_json_value(value)
        return field.has_any_keys(params), value

    @staticmethod
    def contains(field, param=None, value=None, **kwargs):
        value_type, new_value =  encode_json_value(value)
        return field.contains(param), new_value

    @staticmethod
    def contained_by(field, param=None, value=None, **kwargs):
        value_type, new_value =  encode_json_value(value)
        return field.contained_by(param), new_value

    @staticmethod
    def starts_with(field, param=None, value=None, **kwargs):
        new_value = f'"{value}%'
        return functions.Cast(field, SqlTypes.VARCHAR).like(param), new_value

    @staticmethod
    def ends_with(field, param=None, value=None, **kwargs):
        new_value = f'%{value}"'
        return functions.Cast(field, SqlTypes.VARCHAR).like(param), new_value


JSON_FILTER_OPERATORS = {
    'equal': JSONFilterFunctions.equal,
    'not': JSONFilterFunctions.not_equal,
    'has_key': JSONFilterFunctions.has_key,
    'has_keys': JSONFilterFunctions.has_keys,
    'has_anykeys': JSONFilterFunctions.has_anykeys,
    'contains': JSONFilterFunctions.contains,
    'in': JSONFilterFunctions.contained_by,
    'gte': JSONFilterFunctions.greater_equal,
    'gt': JSONFilterFunctions.greater_than,
    'lte': JSONFilterFunctions.less_equal,
    'lt': JSONFilterFunctions.less_than,
    'startswith': JSONFilterFunctions.starts_with,
    'endswith': JSONFilterFunctions.ends_with
}

class JsonFieldFilter:
    def __init__(self, table):
        self.table = table

    def parse_json_key_expr(self, key):
        operator = None
        field = None
        attributes = []
        parts = key.split('__')
        key_name = None
        if len(parts) == 1:
            operator = 'equal'
            key_name = key
        elif len(parts) == 2:
            operator = parts[-1]
            key_name = parts[0]
        elif len(parts) > 2:
            operator = parts[-1]
            key_name = '__'.join(parts[0:-1])

        if key_name:
            nparts = key_name.split('.')
            field = nparts[0]
            attributes = nparts[1:]

        return field, operator, attributes


    def get_criterion(self, key, param_index, value):
        field, operator, attributes = self.parse_json_key_expr(key)
        pika_field = getattr(self.table, field)
        pika_field = JSONField.create(pika_field)
        if len(attributes) == 1:
            name = attributes[0]
            pika_field = pika_field.get_json_value(name)
        elif len(attributes) > 1:
            name = f'{{{",".join(attributes)}}}'
            pika_field = pika_field.get_path_json_value(name)
        operator_func = JSON_FILTER_OPERATORS[operator]
        if operator in ['has_keys', 'has_anykeys']:
            if not isinstance(value, (list, tuple)):
                raise Exception(f'{operator} parameter value must be list or tuple.')
            return operator_func(pika_field, param_index=param_index, value=value)
        else:
            param = parameter(param_index) if isinstance(param_index, int) else param_index
            param = parameter(param_index) if param_index != None else None
            return operator_func(pika_field, param=param, value=value)


class FieldFilterFunctions:

    @staticmethod
    def equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        return operator.eq(field, param_or_value)

    @staticmethod
    def not_equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        return field.ne(param_or_value) | field.isnull()

    @staticmethod
    def greater_equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        return operator.ge(field, param_or_value)

    @staticmethod
    def greater_than(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        return operator.gt(field, param_or_value)

    @staticmethod
    def less_equal(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        return operator.le(field, param_or_value)

    @staticmethod
    def less_than(field, param=None, value=None, **kwargs):
        param_or_value = param or value
        return operator.lt(field, param_or_value)

    @staticmethod
    def is_in(field, param=None, value=None, value_type=str, **kwargs):
        param_or_value = param or value
        return PostgreInCriterion(field.name, param_or_value, value_type)

    @staticmethod
    def not_in(field, param=None, value=None, value_type=str, **kwargs):
        param_or_value = param or value
        return PostgreNotInCriterion(field.name, param_or_value, value_type)

    @staticmethod
    def is_null(field, param=None, value=None, **kwargs):
        if value:
            return field.isnull()
        return field.notnull()

    @staticmethod
    def not_null(field, param=None, value=None, **kwargs):
        if value:
            return field.notnull()
        return field.isnull()

    @staticmethod
    def contains(field, param=None, value=None, **kwargs):
        return functions.Cast(field, SqlTypes.VARCHAR).like(param)

    @staticmethod
    def starts_with(field, param=None, value=None, **kwargs):
        return functions.Cast(field, SqlTypes.VARCHAR).like(param)

    @staticmethod
    def ends_with(field, param=None, value=None, **kwargs):
        return functions.Cast(field, SqlTypes.VARCHAR).like(param)

    @staticmethod
    def insensitive_exact(field, param=None, value=None, **kwargs):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).eq(functions.Upper(param))

    @staticmethod
    def insensitive_contains(field, param=None, value=None, **kwargs):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
            functions.Upper(param)
        )

    @staticmethod
    def insensitive_starts_with(field, param=None, value=None, **kwargs):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
            functions.Upper(param)
        )

    @staticmethod
    def insensitive_ends_with(field, param=None, value=None, **kwargs):
        return functions.Upper(functions.Cast(field, SqlTypes.VARCHAR)).like(
            functions.Upper(param)
        )

FFF = FieldFilterFunctions

class PikaTableFilters:
    filter_funcs_map = {
        'equal': FFF.equal,
        'not_equal': FFF.not_equal,
        'is_in': FFF.is_in,
        'not_in': FFF.not_in,
        'is_null': FFF.is_null,
        'not_null': FFF.not_null,
        'greater_equal': FFF.greater_equal,
        'less_equal': FFF.less_equal,
        'greater_than': FFF.greater_than,
        'less_than': FFF.less_than,
        'contains': FFF.contains,
        'starts_with': FFF.starts_with,
        'ends_with': FFF.ends_with,
        'insensitive_exact': FFF.insensitive_exact,
        'insensitive_contains': FFF.insensitive_contains,
        'insensitive_starts_with': FFF.insensitive_starts_with,
        'insensitive_ends_with': FFF.insensitive_ends_with
    }

    def __init__(self, table, filters):
        self.pika_fields = {}
        self.filters = {}
        self.table = table
        for _, field_filters in filters.items():
            for key, value in field_filters.items():
                db_field = value['db_field']
                if db_field not in self.pika_fields:
                    pika_field = getattr(table, db_field)
                    self.pika_fields[db_field] = pika_field
                else:
                    pika_field = self.pika_fields[db_field]
                operator = value['operator']
                operator_func = self.filter_funcs_map[operator]
                if operator in ['is_in', 'not_in']:
                    operator_func = partial(operator_func, value_type=value['field_type'])
                new_value = deepcopy(value)
                new_value['operator'] = operator_func
                new_value['pika_field'] = pika_field
                self.filters[key] = new_value


    def get_criterion(self, key, param_index, value):
        ff = self.filters.get(key)
        if ff:
            operator_func = ff['operator']
            new_value = value
            if 'value_encoder' in ff:
                new_value = ff['value_encoder'](value)
            param = parameter(param_index) if isinstance(param_index, int) else param_index
            return operator_func(ff['pika_field'], param=param, value=value), new_value
        else:
            return JsonFieldFilter(self.table).get_criterion(key, param_index, value)



class FunctionResolve:
    functions_map = {
        'Trim': functions.Trim,
        'Length': functions.Length,
        'Coalesce': functions.Coalesce,
        'Lower': functions.Lower,
        'Upper': functions.Upper,
        'Count': functions.Count,
        'Sum': functions.Sum,
        'Max': functions.Max,
        'Min': functions.Min,
        'Avg': functions.Avg
    }
    def __init__(self, func):
        self.func = func
        self.func_name = type(func).__name__

    def resolve(self, table):
        func = self.functions_map.get(self.func_name)
        if not func:
            raise Exception(f'no resolver for {self.func_name}')
        args = [getattr(table, self.func.field_name)]
        if self.func.args:
            for arg in self.func.args:
                if isinstance(arg, Function):
                    args.append(FunctionResolve(arg).resolve(table))
                else:
                    args.append(arg)
        return func(*args)

    @classmethod
    def resolve_value(cls, value, table):
        if isinstance(value, Function):
            fn = FunctionResolve(value).resolve(table)
            return fn, None
        else:
            return None, value
