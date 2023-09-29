from typing import Optional
from discoverx.common import helper
from pyspark.sql.types import Row
from dataclasses import dataclass


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    partition_index: int
    classes: list[str]


@dataclass
class TableInfo:
    catalog: Optional[str]
    schema: str
    table: str
    columns: list[ColumnInfo]

    def get_columns_by_class(self, class_name: str):
        return [ClassifiedColumn(col.name, class_name) for col in self.columns if class_name in col.classes]


@dataclass
class ClassifiedColumn:
    name: str
    class_name: str


class InfoFetcher:
    def __init__(self, spark, columns_table_name="system.information_schema.columns") -> None:
        self.columns_table_name = columns_table_name
        self.spark = spark

    def _to_info_list(self, info_rows: list[Row]) -> list[TableInfo]:
        filtered_tables = [
            TableInfo(
                row["table_catalog"],
                row["table_schema"],
                row["table_name"],
                [
                    ColumnInfo(col["column_name"], col["data_type"], col["partition_index"], [])
                    for col in row["table_columns"]
                ],
            )
            for row in info_rows
        ]
        return filtered_tables

    def get_tables_info(self, catalogs: str, schemas: str, tables: str, columns: list[str] = []) -> list[TableInfo]:
        # Filter tables by matching filter
        table_list_sql = self._get_table_list_sql(catalogs, schemas, tables, columns)

        filtered_tables = self.spark.sql(table_list_sql).collect()

        if len(filtered_tables) == 0:
            raise ValueError(f"No tables found matching filter: {catalogs}.{schemas}.{tables}")

        return self._to_info_list(filtered_tables)

    def _get_table_list_sql(self, catalogs: str, schemas: str, tables: str, columns: list[str] = []) -> str:
        """
        Returns a SQL expression which returns a list of columns matching
        the specified filters

        Returns:
            string: The SQL expression
        """

        if "*" in catalogs:
            catalog_sql = f"""AND regexp_like(table_catalog, "^{catalogs.replace("*", ".*")}$")"""
        else:
            catalog_sql = f"""AND table_catalog = "{catalogs}" """

        if "*" in schemas:
            schema_sql = f"""AND regexp_like(table_schema, "^{schemas.replace("*", ".*")}$")"""
        else:
            schema_sql = f"""AND table_schema = "{schemas}" """

        if "*" in tables:
            table_sql = f"""AND regexp_like(table_name, "^{tables.replace("*", ".*")}$")"""
        else:
            table_sql = f"""AND table_name = "{tables}" """

        if columns:
            match_any_col = "|".join([f'({c.replace("*", ".*")})' for c in columns])
            columns_sql = f"""AND regexp_like(column_name, "^{match_any_col}$")"""

        sql = f"""
        WITH tb_list AS (
            SELECT DISTINCT
                table_catalog, 
                table_schema, 
                table_name
            FROM {self.columns_table_name}
            WHERE 
                table_schema != "information_schema" 
                {catalog_sql if catalogs != "*" else ""}
                {schema_sql if schemas != "*" else ""}
                {table_sql if tables != "*" else ""}
                {columns_sql if columns else ""}
        )

        SELECT
            info_schema.table_catalog, 
            info_schema.table_schema, 
            info_schema.table_name, 
            collect_list(struct(column_name, data_type, partition_index)) as table_columns
        FROM {self.columns_table_name} info_schema
        INNER JOIN tb_list ON (
            info_schema.table_catalog <=> tb_list.table_catalog AND
            info_schema.table_schema = tb_list.table_schema AND
            info_schema.table_name = tb_list.table_name)
        GROUP BY info_schema.table_catalog, info_schema.table_schema, info_schema.table_name
        """

        return helper.strip_margin(sql)
