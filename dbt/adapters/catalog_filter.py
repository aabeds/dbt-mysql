from typing import Any, FrozenSet, Tuple, cast

import agate

from dbt.adapters.base.impl import _expect_row_value
from dbt_common.clients.agate_helper import table_from_rows


def catalog_filter_table_for_no_database(
    table: agate.Table, used_schemas: FrozenSet[Tuple[str, str]]
) -> agate.Table:
    """Filter catalog rows for adapters without a database dimension.

    MySQL/MariaDB catalog macros return NULL for table_database, and manifest
    used_schemas may use None for the database component. dbt 1.8's base filter
    calls .lower() on both sides without null checks.
    """
    schemas = frozenset(
        ((database or "").lower(), schema.lower())
        for database, schema in used_schemas
        if schema is not None
    )

    def test(row: agate.Row) -> bool:
        table_schema = _expect_row_value("table_schema", row)
        if table_schema is None:
            return False
        table_database = _expect_row_value("table_database", row) or ""
        return (str(table_database).lower(), str(table_schema).lower()) in schemas

    normalized_table = table_from_rows(
        list(table.rows),
        table.column_names,
        text_only_columns=["table_database", "table_schema", "table_name"],
    )
    return cast(agate.Table, cast(Any, normalized_table).where(test))
