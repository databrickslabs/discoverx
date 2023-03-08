"""This module contains the M-SQL compiler"""
from discoverx.config import ColumnInfo, TableInfo
from discoverx.common.helper import strip_margin
from fnmatch import fnmatch
import re
import itertools

class Msql:
    """This class compiles M-SQL expressions into regular SQL"""
    
    from_statement_expr = r"(FROM\s+)(([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+))"
    tag_regex = r"\[([\w_-]+)\]"

    def __init__(self, msql: str) -> None:
        self.msql = msql

    def compile_msql(self, table_info: TableInfo) -> str:
        """
        Compiles the specified M-SQL (Multiplex-SQL) expression into regular SQL
        Args:
            msql (str): The M-SQL expression

        Returns:
            string: A SQL expression which multiplexes the MSQL expression
        """

        # Replace from clause with table name
        msql = self._replace_from_statement(self.msql, table_info)

        # TODO: Assert alias in SELECT statement
        # non_aliased_tags = re.findall(r"", msql)
        # if non_aliased_tags:
        #     raise ValueError(f"""Tags {non_aliased_tags} are not aliased in M-SQL expression.
        #     Please specify an alias for each tag. Eg. [tag] AS 'my_alias'.""")

        # Find distinct tags in M-SQL expression
        
        tags = list(set(re.findall(self.tag_regex, msql)))

        # Get all columns matching the tags
        columns_by_tag = [table_info.get_columns_by_tag(tag) for tag in tags]

        # Create all possible combinations of tagged columns to be queried
        col_tag_combinations = list(itertools.product(*columns_by_tag))

        # Replace tags in M-SQL expression with column names
        sql_statements = []
        for tagged_cols in col_tag_combinations:
            temp_sql = msql
            for tagged_col in tagged_cols:
                temp_sql = temp_sql.replace(f"[{tagged_col.tag}]", tagged_col.name)
            sql_statements.append(temp_sql)

        # Concatenate all SQL statements
        final_sql = "\nUNION ALL\n".join(sql_statements)

        return strip_margin(final_sql)
    
    def build(self, df, column_type_classification_threshold) -> str:
        (_, _, catalogs, databases, tables) = self._extract_from_components()
        
        classified_cols = df[df['frequency'] > column_type_classification_threshold]
        classified_cols = classified_cols.groupby(['catalog', 'database', 'table', 'column']).aggregate(lambda x: list(x))[['rule_name']].reset_index()

        classified_cols['col_tags'] = classified_cols[['column', 'rule_name']].apply(tuple, axis=1)
        df = classified_cols.groupby(['catalog', 'database', 'table']).aggregate(lambda x: list(x))[['col_tags']].reset_index()

        # Filter tables by matching filter
        filtered_tables = [
            TableInfo(
                row[0], 
                row[1], 
                row[2], 
                [
                    ColumnInfo(
                        col[0], # col name
                        "", # TODO
                        None, # TODO
                        col[1] # Tags
                    ) for col in row[3]
                ]
            ) for _, row in df.iterrows() if fnmatch(row[0], catalogs) and fnmatch(row[1], databases) and fnmatch(row[2], tables)]
        

        sqls = [self.compile_msql(self.msql, table) for table in filtered_tables]
        sql = "\nUNION ALL\n".join(sqls)
        return sql
    
    def _replace_from_statement(self, msql: str, table_info: TableInfo):
        replace_with = f"FROM {table_info.catalog}.{table_info.database}.{table_info.table}"
        
        return re.sub(self.from_statement_expr, replace_with, msql)
    
    def _extract_from_components(self):
        matches = re.findall(self.from_statement_expr, self.msql)
        if len(matches) > 1:
            raise ValueError(f"Multiple FROM statements found in M-SQL expression: {self.msql}")
        elif len(matches) == 1:
            return matches[0]
        else:
            raise ValueError(f"Could not extract table name from M-SQL expression: {self.msql}")