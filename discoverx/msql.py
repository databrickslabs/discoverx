"""This module contains the M-SQL compiler"""
from discoverx.scanner import ColumnInfo, TableInfo, Classifier
from discoverx.common.helper import strip_margin
from fnmatch import fnmatch
import re
import itertools

class Msql:
    """This class compiles M-SQL expressions into regular SQL"""
    
    from_statement_expr = r"(FROM\s+)(([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+).([0-9a-zA-Z_\*]+))"
    command_expr = r"^\s*(\w+)\s"
    tag_regex = r"\[([\w_-]+)\]"
    valid_commands = ["SELECT", "DELETE"]

    def __init__(self, msql: str) -> None:
        self.msql = msql

        # Find distinct tags in M-SQL expression
        self.tags = list(set(re.findall(self.tag_regex, msql)))

        # Extract from clause components
        (self.catalogs, self.databases, self.tables) = self._extract_from_components()

        # Extract command
        self.command = self._extract_command()


    def compile_msql(self, table_info: TableInfo) -> list[str]:
        """
        Compiles the M-SQL (Multiplex-SQL) expression into regular SQL
        Args:
            table_info (TableInfo): Table information

        Returns:
            list[string]: A list of SQL expressions which multiplexes the MSQL expression
        """

        # Replace from clause with table name
        msql = strip_margin(self.msql)
        msql = self._replace_from_statement(msql, table_info)
        msql = self._replace_litaral_keys(msql, table_info)

        # TODO: Assert alias in SELECT statement
        # non_aliased_tags = re.findall(r"", msql)
        # if non_aliased_tags:
        #     raise ValueError(f"""Tags {non_aliased_tags} are not aliased in M-SQL expression.
        #     Please specify an alias for each tag. Eg. [tag] AS 'my_alias'.""")

        # Get all columns matching the tags
        columns_by_tag = [table_info.get_columns_by_tag(tag) for tag in self.tags]

        # Create all possible combinations of tagged columns to be queried
        col_tag_combinations = list(itertools.product(*columns_by_tag))

        # Replace tags in M-SQL expression with column names
        sql_statements = []
        for tagged_cols in col_tag_combinations:
            temp_sql = msql
            for tagged_col in tagged_cols:
                temp_sql = temp_sql.replace(f"[{tagged_col.tag}]", tagged_col.name)
            sql_statements.append(temp_sql)
        
        return sql_statements
    

    def build(self, classifier: Classifier) -> list[str]:

        """Builds the M-SQL expression into a SQL expression"""
        
        classified_cols = classifier.classified_result.copy()
        # TODO: Shouldn't we use the tags from the rule definitions instead of rule_name?
        classified_cols = classified_cols[classified_cols['rule_name'].isin(self.tags)]
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
            ) for _, row in df.iterrows() if fnmatch(row[0], self.catalogs) and fnmatch(row[1], self.databases) and fnmatch(row[2], self.tables)]
        
        if len(filtered_tables) == 0:
            raise ValueError(f"No tables found matching filter: {self.catalogs}.{self.databases}.{self.tables}")

        sqls = flat_map(self.compile_msql, filtered_tables)

        if self.command == "SELECT":
            sqls = ["\nUNION ALL\n".join(sqls)]
        
        return sqls
    


    def _replace_from_statement(self, msql: str, table_info: TableInfo):
        """Replaces the FROM statement in the M-SQL expression with the specified table name"""
        if table_info.catalog and table_info.catalog != "None":
            replace_with = f"FROM {table_info.catalog}.{table_info.database}.{table_info.table}"
        else:
            replace_with = f"FROM {table_info.database}.{table_info.table}"
        
        return re.sub(self.from_statement_expr, replace_with, msql)
    
    def _extract_from_components(self):
        """Extracts the catalog, database and table name from the FROM statement in the M-SQL expression"""
        matches = re.findall(self.from_statement_expr, self.msql)
        if len(matches) > 1:
            raise ValueError(f"Multiple FROM statements found in M-SQL expression: {self.msql}")
        elif len(matches) == 1:
            return (matches[0][2], matches[0][3], matches[0][4])
        else:
            raise ValueError(f"Could not extract table name from M-SQL expression: {self.msql}")
        
    def _replace_litaral_keys(self, msql: str, table_info: TableInfo):
        return (msql
                .replace(r"{catalog_name}", f"'{table_info.catalog}' AS catalog_name")
                .replace(r"{database_name}", f"'{table_info.database}' AS database_name")
                .replace(r"{table_name}", f"'{table_info.table}' AS table_name")
        )
    
    def _extract_command(self):
        """Extracts the command from the M-SQL expression"""
        commands = re.findall(self.command_expr, self.msql)
        if len(commands) != 1:
            raise ValueError(f"Could not extract command from M-SQL expression: {self.msql}. Valid commands are SELECT and DELETE.")
        
        command = commands[0].upper()
        if command not in self.valid_commands:
            raise ValueError(f"Invalid command: {command}. Valid commands are SELECT and DELETE.")
        
        return command
    
def flat_map(f, xs):
    ys = []
    for x in xs:
        ys.extend(f(x))
    return ys