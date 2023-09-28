from typing import Optional, List, Union

from discoverx import logging
from discoverx.msql import Msql
from discoverx.scanner import TableInfo
from discoverx.scanner import Scanner, ScanResult
from discoverx.rules import Rules, Rule
from pyspark.sql import DataFrame, SparkSession

logger = logging.Logging()


class Discovery:
    """ """

    COLUMNS_TABLE_NAME = "system.information_schema.columns"
    MAX_WORKERS = 10

    def __init__(
        self,
        spark: SparkSession,
        catalogs: str,
        schemas: str,
        tables: str,
        table_info_list: list[TableInfo],
        custom_rules: Optional[List[Rule]] = None,
        locale: str = None,
    ):
        self.spark = spark
        self._catalogs = catalogs
        self._schemas = schemas
        self._tables = tables
        self._table_info_list = table_info_list

        self.scanner: Optional[Scanner] = None
        self._scan_result: Optional[ScanResult] = None
        self.rules: Optional[Rules] = Rules(custom_rules=custom_rules, locale=locale)

    def _msql(self, msql: str, what_if: bool = False, min_score: Optional[float] = None):
        logger.debug(f"Executing sql template: {msql}")

        msql_builder = Msql(msql)

        # check if classification is available
        # Check for more specific exception
        classification_result_pdf = self._scan_result.get_classes(min_score)
        sql_rows = msql_builder.build(classification_result_pdf)

        if what_if:
            logger.friendly(f"SQL that would be executed:")

            for sql_row in sql_rows:
                logger.friendly(sql_row.sql)

            return None
        else:
            logger.debug(f"Executing SQL:\n{sql_rows}")
            return msql_builder.execute_sql_rows(sql_rows, self.spark)

    def scan(
        self,
        rules="*",
        sample_size=10000,
        what_if: bool = False,
    ):

        self.scanner = Scanner(
            self.spark,
            self.rules,
            catalogs=self._catalogs,
            schemas=self._schemas,
            tables=self._tables,
            table_list=self._table_info_list,
            rule_filter=rules,
            sample_size=sample_size,
            what_if=what_if,
            columns_table_name=self.COLUMNS_TABLE_NAME,
            max_workers=self.MAX_WORKERS,
        )

        self._scan_result = self.scanner.scan()
        logger.friendlyHTML(self.scanner.summary_html)

    def _check_scan_result(self):
        if self._scan_result is None:
            raise Exception("You first need to scan your lakehouse using Scanner.scan()")

    @property
    def scan_result(self):
        """Returns the scan results as a pandas DataFrame

        Raises:
            Exception: If the scan has not been run
        """
        self._check_scan_result()

        return self._scan_result.df

    def search(
        self,
        search_term: str,
        from_tables: str = "*.*.*",
        by_class: Optional[str] = None,
        min_score: Optional[float] = None,
    ):
        """Searches your lakehouse for columns matching the given search term

        Args:
            search_term (str): The search term to be used to search for columns.
            from_tables (str, optional): The tables to be searched in format
                "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_class (str, optional): The class to be used to search for columns.
                Defaults to None.
            min_score (float, optional): Defines the classification score or frequency
                threshold for columns to be considered during the scan. Defaults to None
                which means that all columns where at least one record matched the
                respective rule during the scan will be included. Has to be either None
                or between 0 and 1.

        Raises:
            ValueError: If search_term is not provided
            ValueError: If the search_term type is not valid
            ValueError: If the by_class type is not valid

        Returns:
            DataFrame: A dataframe containing the results of the search
        """

        Msql.validate_from_components(from_tables)

        if search_term is None:
            raise ValueError("search_term has not been provided.")

        if not isinstance(search_term, str):
            raise ValueError(f"The search_term type {type(search_term)} is not valid. Please use a string type.")

        if by_class is None:
            # Trying to infer the class by the search term
            logger.friendly(
                "You did not provide any class to be searched."
                "We will try to auto-detect matching rules for the given search term"
            )
            search_matching_rules = self.rules.match_search_term(search_term)
            if len(search_matching_rules) == 0:
                raise ValueError(
                    f"Could not infer any class for the given search term. Please specify the by_class parameter."
                )
            elif len(search_matching_rules) > 1:
                raise ValueError(
                    f"Multiple classes {search_matching_rules} match the given search term ({search_term}). Please specify the class to search in with the by_class parameter."
                )
            else:
                by_class = search_matching_rules[0]
            logger.friendly(f"Discoverx will search your lakehouse using the class {by_class}")
        elif isinstance(by_class, str):
            search_matching_rules = [by_class]
        else:
            raise ValueError(f"The provided by_class {by_class} must be of string type.")

        sql_filter = f"`[{search_matching_rules[0]}]` = '{search_term}'"
        select_statement = (
            "named_struct("
            + ", ".join(
                [
                    f"'{rule_name}', named_struct('column_name', '[{rule_name}]', 'value', `[{rule_name}]`)"
                    for rule_name in search_matching_rules
                ]
            )
            + ") AS search_result"
        )

        where_statement = f"WHERE {sql_filter}"

        return self._msql(
            f"SELECT {select_statement}, to_json(struct(*)) AS row_content FROM {from_tables} {where_statement}",
            min_score=min_score,
        )

    def select_by_classes(
        self,
        from_tables: str = "*.*.*",
        by_classes: Optional[Union[List[str], str]] = None,
        min_score: Optional[float] = None,
    ):
        """Selects all columns in the lakehouse from tables that match ALL the given classes

        Args:
            from_tables (str, optional): The tables to be selected in format
                "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_classes (Union[List[str], str], optional): The classes to be used to
                search for columns. Defaults to None.
            min_score (float, optional): Defines the classification score or frequency
                threshold for columns to be considered during the scan. Defaults to None
                which means that all columns where at least one record matched the
                respective rule during the scan will be included. Has to be either None
                or between 0 and 1.

        Raises:
            ValueError: If the by_classes type is not valid

        Returns:
            DataFrame: A dataframe containing the UNION ALL results of the select"""

        Msql.validate_from_components(from_tables)

        if isinstance(by_classes, str):
            by_classes = [by_classes]
        elif isinstance(by_classes, list) and all(isinstance(elem, str) for elem in by_classes):
            by_classes = by_classes
        else:
            raise ValueError(
                f"The provided by_classes {by_classes} have the wrong type. Please provide"
                f" either a str or List[str]."
            )

        from_statement = (
            "named_struct("
            + ", ".join(
                [
                    f"'{class_name}', named_struct('column_name', '[{class_name}]', 'value', `[{class_name}]`)"
                    for class_name in by_classes
                ]
            )
            + ") AS classified_columns"
        )

        return self._msql(
            f"SELECT {from_statement}, to_json(struct(*)) AS row_content FROM {from_tables}", min_score=min_score
        )

    def delete_by_class(
        self,
        from_tables="*.*.*",
        by_class: str = None,
        values: Optional[Union[List[str], str]] = None,
        yes_i_am_sure: bool = False,
        min_score: Optional[float] = None,
    ):
        """Deletes all rows in the lakehouse that match any of the provided values in a column classified with the given class

        Args:
            from_tables (str, optional): The tables to delete from in format
                "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_class (str, optional): The class to be used to search for columns.
                Defaults to None.
            values (Union[List[str], str], optional): The values to be deleted.
                Defaults to None.
            yes_i_am_sure (bool, optional): Whether you are sure that you want to delete
                the data. If False prints the SQL statements instead of executing them. Defaults to False.
            min_score (float, optional): Defines the classification score or frequency
                threshold for columns to be considered during the scan. Defaults to None
                which means that all columns where at least one record matched the
                respective rule during the scan will be included. Has to be either None
                or between 0 and 1.

        Raises:
            ValueError: If the from_tables is not valid
            ValueError: If the by_class is not valid
            ValueError: If the values is not valid
        """

        Msql.validate_from_components(from_tables)

        if (by_class is None) or (not isinstance(by_class, str)):
            raise ValueError(f"Please provide a class to identify the columns to be matched on the provided values.")

        if values is None:
            raise ValueError(
                f"Please specify the values to be deleted. You can either provide a list of values or a single value."
            )
        elif isinstance(values, str):
            value_string = f"'{values}'"
        elif isinstance(values, list) and all(isinstance(elem, str) for elem in values):
            value_string = "'" + "', '".join(values) + "'"
        else:
            raise ValueError(
                f"The provided values {values} have the wrong type. Please provide" f" either a str or List[str]."
            )

        if not yes_i_am_sure:
            logger.friendly(
                f"Please confirm that you want to delete the following values from the table {from_tables} using the class {by_class}: {values}"
            )
            logger.friendly(
                f"If you are sure, please run the same command again but set the parameter yes_i_am_sure to True."
            )

        delete_result = self._msql(
            f"DELETE FROM {from_tables} WHERE `[{by_class}]` IN ({value_string})",
            what_if=(not yes_i_am_sure),
            min_score=min_score,
        )

        if delete_result is not None:
            delete_result = delete_result.toPandas()
            logger.friendlyHTML(f"<p>The affected tables are</p>{delete_result.to_html()}")

    def save_scan(self):
        """Method to save scan result"""
        # TODO:
        pass
