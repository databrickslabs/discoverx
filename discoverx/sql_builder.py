"""
This module contains classes and functions which automatically build
SQL expressions for specified tables and rules
"""
from discoverx.config import TableInfo, Rule
from discoverx.common import trim_sql


class SqlBuilder:
    """
    The SqlBuilder class automatically creates a SQL expression for
    scanning tables and determining the likelihood of its columns to
    match specified rules
    """
    # TODO: move table_info, rules, ... to an init method
    # pylint: disable=too-few-public-methods
    def rule_matching_sql(self, table_info: TableInfo, rules: list[Rule], sample_size: int = 1000):
        """
        Given a table and a set of rules this method will return a
        SQL expression which matches the table's columns against rules.
        If executed on the table using SQL the output will contain a
        matching frequency (probability) for each column and rule.
        Args:
            table_info (TableInfo): Specifies the table to be scanned
            rules (list[Rule]): A list of Rules based on regular
                expressions
            sample_size (int): The number of records sampled/scanned
                for each table

        Returns:
            string: The SQL expression

        """

        expressions = [r for r in rules if r.type == "regex"]
        cols = [c for c in table_info.columns if c.data_type == "string"]

        matching_columns = [f"INT(regexp_like(value, '{r.definition}')) AS {r.name}" for r in expressions]
        matching_string = ",\n                    ".join(matching_columns)

        unpivot_expressions = ", ".join([f"'{r.name}', {r.name}" for r in expressions])
        unpivot_columns = ", ".join([f"'{c.name}', {c.name}" for c in cols])

        sql = f"""
            SELECT 
                '{table_info.catalog}' as catalog,
                '{table_info.database}' as database,
                '{table_info.table}' as table, 
                column,
                rule_name,
                (sum(value) / count(value)) as frequency
            FROM
            (
                SELECT column, stack({len(expressions)}, {unpivot_expressions}) as (rule_name, value)
                FROM 
                (
                    SELECT
                    column,
                    {matching_string}
                    FROM (
                    SELECT
                        stack({len(cols)}, {unpivot_columns}) AS (column, value)
                    FROM {table_info.database}.{table_info.table}
                    TABLESAMPLE ({sample_size} ROWS)
                    )
                )
            )
            GROUP BY catalog, database, table, column, rule_name
        """

        return trim_sql(sql)
