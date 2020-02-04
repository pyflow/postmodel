
from hashlib import sha256
from typing import List, Set


class BaseTableSchemaGenerator:
    DIALECT = "sql"
    TABLE_CREATE_TEMPLATE = 'CREATE TABLE {exists}"{table_name}" ({fields}){extra}{comment};'
    FIELD_TEMPLATE = '"{name}" {type} {nullable} {unique}{primary}{comment}'
    INDEX_CREATE_TEMPLATE = 'CREATE INDEX {exists}"{index_name}" ON "{table_name}" ({fields});'
    UNIQUE_CONSTRAINT_CREATE_TEMPLATE = 'CONSTRAINT "{index_name}" UNIQUE ({fields})'

    def __init__(self, meta_info) -> None:
        self.meta_info = meta_info

    def get_create_schema_sql(self, safe=True) -> str:
        exists="IF NOT EXISTS " if safe else ""
        table_name = self.meta_info.table
        return self.TABLE_CREATE_TEMPLATE.format(
            exists = exists,
            table_name = table_name,
            fields = "",
            extra = "",
            comment = ""
        )

