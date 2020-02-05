
from hashlib import sha256
from typing import List, Set


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
        'BinaryField': "BLOB"
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

