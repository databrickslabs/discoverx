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
class TagInfo:
    tag_name: str
    tag_value: str


@dataclass
class ColumnTagInfo:
    column_name: str
    tag_name: str
    tag_value: str


@dataclass
class TagsInfo:
    column_tags: Optional[list[ColumnTagInfo]]
    table_tags: Optional[list[TagInfo]]
    schema_tags: Optional[list[TagInfo]]
    catalog_tags: Optional[list[TagInfo]]


@dataclass
class TableInfo:
    catalog: Optional[str]
    schema: str
    table: str
    columns: list[ColumnInfo]
    tags: Optional[TagsInfo]

    def get_columns_by_class(self, class_name: str):
        return [ClassifiedColumn(col.name, class_name) for col in self.columns if class_name in col.classes]


@dataclass
class ClassifiedColumn:
    name: str
    class_name: str


class InfoFetcher:
    def __init__(self, spark, information_schema="system.information_schema") -> None:
        self.information_schema = information_schema
        self.spark = spark

    @staticmethod
    def _get_tag_info(row: Row) -> TagsInfo:
        if all(item in row.asDict().keys() for item in ["column_tags", "table_tags", "schema_tags", "catalog_tags"]):
            if row["column_tags"] is None:
                column_tags = []
            else:
                column_tags = [
                    ColumnTagInfo(tag["column_name"], tag["tag_name"], tag["tag_value"]) for tag in row["column_tags"]
                ]

            if row["table_tags"] is None:
                table_tags = []
            else:
                table_tags = [TagInfo(tag["tag_name"], tag["tag_value"]) for tag in row["table_tags"]]

            if row["schema_tags"] is None:
                schema_tags = []
            else:
                schema_tags = [TagInfo(tag["tag_name"], tag["tag_value"]) for tag in row["schema_tags"]]

            if row["catalog_tags"] is None:
                catalog_tags = []
            else:
                catalog_tags = [TagInfo(tag["tag_name"], tag["tag_value"]) for tag in row["catalog_tags"]]

            tags = TagsInfo(column_tags, table_tags, schema_tags, catalog_tags)
        else:
            tags = None
        return tags

    @staticmethod
    def _to_info_row(row: Row) -> TableInfo:
        columns = [
            ColumnInfo(col["column_name"], col["data_type"], col["partition_index"], []) for col in row["table_columns"]
        ]

        return TableInfo(
            row["table_catalog"],
            row["table_schema"],
            row["table_name"],
            columns=columns,
            tags=InfoFetcher._get_tag_info(row),
        )

    def _to_info_list(self, info_rows: list[Row]) -> list[TableInfo]:
        filtered_tables = [self._to_info_row(row) for row in info_rows]
        return filtered_tables

    def get_tables_info(
        self,
        catalogs: str,
        schemas: str,
        tables: str,
        columns: list[str] = [],
        with_tags: bool = False,
        having_tags: list[TagInfo] = [],
        data_source_formats: list[str] = ["DELTA"],
    ) -> list[TableInfo]:
        # Filter tables by matching filter
        table_list_sql = self._get_table_list_sql(catalogs, schemas, tables, columns, with_tags, data_source_formats)

        filtered_tables = self.spark.sql(table_list_sql).collect()

        if len(filtered_tables) == 0:
            raise ValueError(f"No tables found matching filter: {catalogs}.{schemas}.{tables}")

        info_list = self._to_info_list(filtered_tables)
        return [info for info in info_list if InfoFetcher._contains_all_tags(info.tags, having_tags)]

    @staticmethod
    def _contains_all_tags(tags_info: TagsInfo, tags: list[TagInfo]) -> bool:
        if not tags:
            return True
        if not tags_info:
            return False

        all_tags = []

        if tags_info.catalog_tags:
            all_tags.extend(tags_info.catalog_tags)

        if tags_info.schema_tags:
            all_tags.extend(tags_info.schema_tags)

        if tags_info.table_tags:
            all_tags.extend(tags_info.table_tags)

        return all([tag in all_tags for tag in tags])

    def _get_table_list_sql(
        self,
        catalogs: str,
        schemas: str,
        tables: str,
        columns: list[str] = [],
        with_tags=False,
        data_source_formats: list[str] = ["DELTA"],
    ) -> str:
        """
        Returns a SQL expression which returns a list of columns matching
        the specified filters

        Returns:
            string: The SQL expression
        """

        if "*" in catalogs:
            catalog_sql = f"""AND regexp_like(table_catalog, "^{catalogs.replace("*", ".*")}$")"""
            catalog_tags_sql = f"""AND regexp_like(catalog_name, "^{catalogs.replace("*", ".*")}$")"""
        else:
            catalog_sql = f"""AND table_catalog = "{catalogs}" """
            catalog_tags_sql = f"""AND catalog_name = "{catalogs}" """

        if "*" in schemas:
            schema_sql = f"""AND regexp_like(table_schema, "^{schemas.replace("*", ".*")}$")"""
            schema_tags_sql = f"""AND regexp_like(schema_name, "^{schemas.replace("*", ".*")}$")"""
        else:
            schema_sql = f"""AND table_schema = "{schemas}" """
            schema_tags_sql = f"""AND schema_name = "{schemas}" """

        if "*" in tables:
            table_sql = f"""AND regexp_like(table_name, "^{tables.replace("*", ".*")}$")"""
        else:
            table_sql = f"""AND table_name = "{tables}" """

        if columns:
            match_any_col = "|".join([f'({c.replace("*", ".*")})' for c in columns])
            columns_sql = f"""AND regexp_like(column_name, "^{match_any_col}$")"""

        data_source_formats_values = ",".join("'{0}'".format(f).upper() for f in data_source_formats)

        with_column_info_sql = f"""
        WITH all_user_tbl_list AS (
            SELECT DISTINCT
                table_catalog, 
                table_schema, 
                table_name
            FROM {self.information_schema}.columns
            WHERE 
                table_schema != "information_schema" 
                {catalog_sql if catalogs != "*" else ""}
                {schema_sql if schemas != "*" else ""}
                {table_sql if tables != "*" else ""}
                {columns_sql if columns else ""}
        ),
        
        req_tbl_list AS (
        SELECT DISTINCT
            table_catalog,
            table_schema,
            table_name
            FROM {self.information_schema}.tables
            WHERE
                table_schema != "information_schema"
                {catalog_sql if catalogs != "*" else ""}
                {schema_sql if schemas != "*" else ""}
                {table_sql if tables != "*" else ""}
                and table_type in ('MANAGED','EXTERNAL','MANAGED_SHALLOW_CLONE','EXTERNAL_SHALLOW_CLONE')
                and data_source_format in ({data_source_formats_values})
        ),
        
        filtered_tbl_list AS (
            SELECT a.* 
            FROM all_user_tbl_list a
            INNER JOIN
            req_tbl_list r ON(
            a.table_catalog <=> r.table_catalog AND
            a.table_schema = r.table_schema AND
            a.table_name = r.table_name
            )
        
        ),

        col_list AS (
          SELECT
            info_schema.table_catalog,
            info_schema.table_schema,
            info_schema.table_name,
            collect_list(struct(column_name, data_type, partition_index)) as table_columns
          FROM {self.information_schema}.columns info_schema
          WHERE 
                table_schema != "information_schema" 
                {catalog_sql if catalogs != "*" else ""}
                {schema_sql if schemas != "*" else ""}
                {table_sql if tables != "*" else ""}
          GROUP BY info_schema.table_catalog, info_schema.table_schema, info_schema.table_name
        ),

        with_column_info AS (
          SELECT
              col_list.*
          FROM col_list
          INNER JOIN filtered_tbl_list ON (
              col_list.table_catalog <=> filtered_tbl_list.table_catalog AND
              col_list.table_schema = filtered_tbl_list.table_schema AND
              col_list.table_name = filtered_tbl_list.table_name)
        )

        """

        tags_sql = f"""
        ,
        catalog_tags AS (
          SELECT
            info_schema.catalog_name AS table_catalog,
            collect_list(struct(tag_name, tag_value)) as catalog_tags
          FROM {self.information_schema}.catalog_tags info_schema
          WHERE 
                catalog_name != "system"
                {catalog_tags_sql if catalogs != "*" else ""}
          GROUP BY info_schema.catalog_name
        ),

        schema_tags AS (
          SELECT
            info_schema.catalog_name AS table_catalog,
            info_schema.schema_name AS table_schema,
            collect_list(struct(tag_name, tag_value)) as schema_tags
          FROM {self.information_schema}.schema_tags info_schema
          WHERE 
                schema_name != "information_schema" 
                {catalog_tags_sql if catalogs != "*" else ""}
                {schema_tags_sql if schemas != "*" else ""}
          GROUP BY info_schema.catalog_name, info_schema.schema_name
        ),

        table_tags AS (
          SELECT
            info_schema.catalog_name AS table_catalog,
            info_schema.schema_name AS table_schema,
            info_schema.table_name,
            collect_list(struct(tag_name, tag_value)) as table_tags
          FROM {self.information_schema}.table_tags info_schema
          WHERE 
                schema_name != "information_schema" 
                {catalog_tags_sql if catalogs != "*" else ""}
                {schema_tags_sql if schemas != "*" else ""}
                {table_sql if tables != "*" else ""}
          GROUP BY info_schema.catalog_name, info_schema.schema_name, info_schema.table_name
        ),

        column_tags AS (
          SELECT
            info_schema.catalog_name AS table_catalog,
            info_schema.schema_name AS table_schema,
            info_schema.table_name,
            collect_list(struct(column_name, tag_name, tag_value)) as column_tags
          FROM {self.information_schema}.column_tags info_schema
          WHERE 
                schema_name != "information_schema" 
                {catalog_tags_sql if catalogs != "*" else ""}
                {schema_tags_sql if schemas != "*" else ""}
                {table_sql if tables != "*" else ""}
          GROUP BY info_schema.catalog_name, info_schema.schema_name, info_schema.table_name
        ),

        tags AS (
          SELECT 
            filtered_tbl_list.table_catalog,
            filtered_tbl_list.table_schema,
            filtered_tbl_list.table_name,
            catalog_tags.catalog_tags,
            schema_tags.schema_tags,
            table_tags.table_tags,
            column_tags.column_tags
          FROM filtered_tbl_list
          LEFT OUTER JOIN table_tags ON (
            table_tags.table_catalog <=> filtered_tbl_list.table_catalog AND 
            table_tags.table_schema = filtered_tbl_list.table_schema AND 
            table_tags.table_name = filtered_tbl_list.table_name
            )
          LEFT OUTER JOIN schema_tags
          ON filtered_tbl_list.table_catalog <=> schema_tags.table_catalog AND filtered_tbl_list.table_schema = schema_tags.table_schema 
          LEFT OUTER JOIN column_tags
          ON filtered_tbl_list.table_catalog <=> column_tags.table_catalog AND filtered_tbl_list.table_schema = column_tags.table_schema AND filtered_tbl_list.table_name = column_tags.table_name
          LEFT OUTER JOIN catalog_tags
          ON catalog_tags.table_catalog <=> filtered_tbl_list.table_catalog
        )

        
        """

        if with_tags:
            sql = (
                with_column_info_sql
                + tags_sql
                + f"""
            SELECT
                with_column_info.*,
                tags.table_tags,
                tags.catalog_tags,
                tags.schema_tags,
                tags.table_tags,
                tags.column_tags
            FROM with_column_info
            LEFT OUTER JOIN tags ON (
              with_column_info.table_catalog <=> tags.table_catalog AND
              with_column_info.table_schema = tags.table_schema AND
              with_column_info.table_name = tags.table_name)
            """
            )
        else:
            sql = (
                with_column_info_sql
                + f"""
                SELECT
                *
                FROM with_column_info
                """
            )

        return helper.strip_margin(sql)
