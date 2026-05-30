from typing import FrozenSet, Optional, List, Tuple
import agate

from dbt.adapters.base.column import Column as BaseColumn
from dbt.adapters.base.impl import ConstraintSupport
from dbt.adapters.base import BaseRelation
from dbt.adapters.catalog_filter import catalog_filter_table_for_no_database
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.mysql5.column import MySQLColumn
from dbt.adapters.mysql5.connections import MySQLConnectionManager
from dbt.adapters.mysql5.relation import MySQLRelation
from dbt.adapters.sql import SQLAdapter
from dbt_common.contracts.constraints import ConstraintType
from dbt_common.exceptions import DbtRuntimeError

logger = AdapterLogger("mysql")

LIST_SCHEMAS_MACRO_NAME = "list_schemas"
LIST_RELATIONS_MACRO_NAME = "list_relations_without_caching"


class MySQLAdapter(SQLAdapter):
    Relation = MySQLRelation
    Column = MySQLColumn
    ConnectionManager = MySQLConnectionManager

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        # While Foreign Keys are indeed supported, they're not supported in
        # CREATE TABLE AS SELECT statements, which is what DBT uses.
        #
        # It is possible to use a `post-hook` to add a foreign key after the
        # table is created.
        ConstraintType.foreign_key: ConstraintSupport.NOT_SUPPORTED,
    }

    @classmethod
    def _catalog_filter_table(
        cls, table: agate.Table, used_schemas: FrozenSet[Tuple[str, str]]
    ) -> agate.Table:
        return catalog_filter_table_for_no_database(table, used_schemas)

    @classmethod
    def date_function(cls):
        return "current_date()"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "timestamp"

    @classmethod
    def quote(cls, identifier: str) -> str:
        return "`{}`".format(identifier)

    def list_relations_without_caching(  # type: ignore[override]
        self, schema_relation: MySQLRelation  # type: ignore[override]
    ) -> List[MySQLRelation]:
        kwargs = {"schema_relation": schema_relation}
        try:
            results = self.execute_macro(LIST_RELATIONS_MACRO_NAME, kwargs=kwargs)
        except DbtRuntimeError as e:
            errmsg = getattr(e, "msg", "")
            if f"MySQL database '{schema_relation}' not found" in errmsg:
                return []
            else:
                description = "Error while retrieving information about"
                logger.debug(f"{description} {schema_relation}: {e.msg}")
                return []

        relations = []
        for row in results:
            if len(row) != 4:
                raise DbtRuntimeError(
                    "Invalid value from "
                    f'"mysql5__list_relations_without_caching({kwargs})", '
                    f"got {len(row)} values, expected 4"
                )
            _, name, _schema, relation_type = row
            relation = self.Relation.create(schema=_schema, identifier=name, type=relation_type)
            relations.append(relation)

        return relations

    def get_columns_in_relation(self, relation: BaseRelation) -> List[MySQLColumn]:  # type: ignore[override]
        columns: List[BaseColumn] = super().get_columns_in_relation(relation)
        return self.parse_show_columns(relation, columns)

    def get_relation(
        self, database: Optional[str], schema: str, identifier: str
    ) -> Optional[BaseRelation]:
        if not self.Relation.get_default_include_policy().database:
            database = None

        return super().get_relation(database, schema, identifier)

    def parse_show_columns(
        self, relation: BaseRelation, raw_columns: List[BaseColumn]
    ) -> List[MySQLColumn]:
        return [
            MySQLColumn(
                table_database=None,
                table_schema=relation.schema,
                table_name=relation.name,
                table_type=relation.type,
                table_owner=None,
                table_stats=None,
                column=col.column,
                column_index=idx,
                dtype=col.dtype,
            )
            for idx, col in enumerate(raw_columns)
        ]

    def check_schema_exists(self, database, schema):
        results = self.execute_macro(LIST_SCHEMAS_MACRO_NAME, kwargs={"database": database})

        exists = True if schema in [row[0] for row in results] else False
        return exists

    # Methods used in adapter tests
    def update_column_sql(
        self,
        dst_name: str,
        dst_column: str,
        clause: str,
        where_clause: Optional[str] = None,
    ) -> str:
        clause = f"update {dst_name} set {dst_column} = {clause}"
        if where_clause is not None:
            clause += f" where {where_clause}"
        return clause

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        # for backwards compatibility, we're compelled to set some sort of
        # default. A lot of searching has lead me to believe that the
        # '+ interval' syntax used in postgres/redshift is relatively common
        # and might even be the SQL standard's intention.
        return f"date_add({add_to}, interval {number} {interval})"

    def string_add_sql(
        self,
        add_to: str,
        value: str,
        location="append",
    ) -> str:
        if location == "append":
            return f"concat({add_to}, '{value}')"
        elif location == "prepend":
            return f"concat({value}, '{add_to}')"
        else:
            raise DbtRuntimeError(f'Got an unexpected location value of "{location}"')

    def get_rows_different_sql(
        self,
        relation_a: MySQLRelation,  # type: ignore[override]
        relation_b: MySQLRelation,  # type: ignore[override]
        column_names: Optional[List[str]] = None,
        except_operator: str = "",  # Required to match BaseRelation.get_rows_different_sql()
    ) -> str:
        # This method only really exists for test reasons
        names: List[str]
        if column_names is None:
            columns = self.get_columns_in_relation(relation_a)
            names = sorted((self.quote(c.name) for c in columns))
        else:
            names = sorted((self.quote(n) for n in column_names))

        alias_a = "A"
        alias_b = "B"
        columns_csv_a = ", ".join([f"{alias_a}.{name}" for name in names])
        columns_csv_b = ", ".join([f"{alias_b}.{name}" for name in names])
        join_condition = " AND ".join([f"{alias_a}.{name} = {alias_b}.{name}" for name in names])
        first_column = names[0]

        # MySQL doesn't have an EXCEPT or MINUS operator,
        # so we need to simulate it
        COLUMNS_EQUAL_SQL = """
        SELECT
            row_count_diff.difference as row_count_difference,
            diff_count.num_missing as num_mismatched
        FROM (
            SELECT
                1 as id,
                table_a.num_rows - table_b.num_rows as difference
            FROM
                (SELECT COUNT(*) as num_rows FROM {relation_a}) as table_a,
                (SELECT COUNT(*) as num_rows FROM {relation_b}) as table_b
            ) as row_count_diff
        INNER JOIN (
            SELECT
                1 as id,
                COUNT(*) as num_missing FROM (

                    SELECT
                        {columns_a}
                    FROM {relation_a} as {alias_a}
                    LEFT OUTER JOIN {relation_b} as {alias_b}
                        ON {join_condition}
                    WHERE {alias_b}.{first_column} is null

                    UNION ALL

                    SELECT
                        {columns_b}
                    FROM {relation_b} as {alias_b}
                    LEFT OUTER JOIN {relation_a} as {alias_a}
                        ON {join_condition}
                    WHERE {alias_a}.{first_column} is null

                ) as missing
            ) as diff_count ON row_count_diff.id = diff_count.id
        """.strip()

        sql = COLUMNS_EQUAL_SQL.format(
            alias_a=alias_a,
            alias_b=alias_b,
            first_column=first_column,
            columns_a=columns_csv_a,
            columns_b=columns_csv_b,
            join_condition=join_condition,
            relation_a=str(relation_a),
            relation_b=str(relation_b),
        )

        return sql
