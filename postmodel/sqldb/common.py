
from hashlib import sha256
from typing import List, Set
import operator
from pypika import functions
from pypika.enums import SqlTypes
from pypika.terms import Criterion
from functools import partial
from copy import deepcopy
from postmodel.models.functions import Function

class BaseTableSchemaGenerator:
    DIALECT = "sql"
    TABLE_CREATE_TEMPLATE = 'CREATE TABLE {exists}"{table_name}" ({fields}){extra}{comment};'
    TABLE_DELETE_TEMPLATE = 'DROP TABLE "{table_name}" CASCADE;'
    TABLE_RENAME_TEMPLATE = 'ALTER TABLE "{table_name}" RENAME TO "{new_table_name}";'

    FIELD_TEMPLATE = '"{name}" {type} {nullable} {unique}{primary}{comment}'
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
            is_pk = field.pk
            field_type = self.FIELD_TYPE_MAP[type(field).__name__]
            if callable(field_type):
                field_type = field_type(field)
            if field.index and not field.pk:
                fields_with_index.append(field)
            sql = self.FIELD_TEMPLATE.format(
                name=db_field,
                type=field_type,
                nullable=nullable,
                unique="" if field.pk else unique,
                comment= "",
                primary=" PRIMARY KEY" if is_pk else "",
            ).strip()
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

    def get_criterion(self, key, param, value):
        ff = self.filters.get(key)
        operator_func = ff['operator']
        new_value = value
        if 'value_encoder' in ff:
            new_value = ff['value_encoder'](value)
        return operator_func(ff['pika_field'], param=param, value=value), new_value


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
