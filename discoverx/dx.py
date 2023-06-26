from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from typing import List, Optional, Union
from discoverx import logging
from discoverx.msql import Msql
from discoverx.rules import Rules, Rule
from discoverx.scanner import Scanner
from discoverx.classification import Classifier


class DX:
    """DiscoverX scans and searches your lakehouse

    DiscoverX scans your data for patterns which have been pre-defined
    as rules. You can either use standard rules which come with
    DiscoverX or define and add custom rules.
    Attributes:
        custom_rules (List[Rule], Optional): Custom rules which will be
            used to detect columns with corresponding patterns in your
            data
        classification_threshold (float, optional):
            The threshold which will associate a column with a specific
            rule and classify accordingly. The minimum and maximum
            threshold values which can be specified are 0 and 1
            respectively. The former corresponds to none of the records
            for that column conforming to the given rule while the
            latter means that all records conform.
        spark (SparkSession, optional): The SparkSession which will be
            used to scan your data. Defaults to None.
        locale (str, optional): The two-letter country code which will be
            used to determine the localized scanning rules.
            Defaults to None.
    """

    COLUMNS_TABLE_NAME = "system.information_schema.columns"
    MAX_WORKERS = 10

    def __init__(
        self,
        custom_rules: Optional[List[Rule]] = None,
        classification_threshold: float = 0.95,
        spark: Optional[SparkSession] = None,
        locale: str = None,
        classification_table_name: str = "_discoverx.classification.classes",
    ):

        if spark is None:
            spark = SparkSession.getActiveSession()
        self.spark = spark
        self.logger = logging.Logging()

        self.rules = Rules(custom_rules=custom_rules, locale=locale)

        self.classification_threshold = self._validate_classification_threshold(classification_threshold)
        self.classification_table_name = classification_table_name

        self.uc_enabled = self.spark.conf.get("spark.databricks.unityCatalog.enabled", "false") == "true"
        self.can_read_columns_table = self.can_read_columns_table()

        self.scanner: Optional[Scanner] = None
        self.classifier: Optional[Classifier] = None

        self.intro()

    def can_read_columns_table(self) -> bool:
        try:
            self.spark.sql(f"SELECT * FROM {self.COLUMNS_TABLE_NAME} LIMIT 1")
            return True
        except Exception as e:
            self.logger.error(f"Error while reading table {self.COLUMNS_TABLE_NAME}: {e}")
            return False

    def intro(self):
        # TODO: Decide on how to do the introduction
        intro_text = """
        <h1>Hi there, I'm DiscoverX.</h1>

        <p>
          I'm here to help you discover data in your lakehouse.<br />
          You can scan your lakehouse by using
        </p>
        <pre><code>dx.scan(from_tables="*.*.*")</code></pre>
        <p>
          For more detailed instructions, check out the <a href="https://github.com/databrickslabs/discoverx">readme</a> or use
        </p>
        <pre><code>help(DX)</code></pre>
        """

        missing_uc_text = """
        <h1 style="color: red">Uch! DiscoverX needs Unity Catalog to be enabled</h1>

        <p>
          Please make sure you have Unity Catalog enabled, and that you are running a Cluster that supports Unity Catalog.
        </p>
        """

        missing_access_to_columns_table_text = """
        <h1 style="color: red">DiscoverX needs access to the system tables `system.information_schema.columns`</h1>
        
        <p>
            Please make sure you have access to the system tables `system.information_schema.columns` and that you are running a Cluster that supports Unity Catalog.
            To grant access to the system tables, execute the following commands:<br />
            <code>GRANT USE CATALOG ON CATALOG system TO `account users`;</code><br /> 
            <code>GRANT USE SCHEMA ON CATALOG system TO `account users`;</code><br />
            <code>GRANT SELECT ON CATALOG system TO `account users`;</code>
        </p>
        """

        if not self.uc_enabled:
            self.logger.friendlyHTML(missing_uc_text)
        if not self.can_read_columns_table:
            self.logger.friendlyHTML(missing_access_to_columns_table_text)
        else:
            self.logger.friendlyHTML(intro_text)

    def display_rules(self):
        """Displays the available rules in a friendly HTML format"""
        text = self.rules.get_rules_info()
        self.logger.friendlyHTML(text)

    def scan(
        self,
        from_tables="*.*.*",
        rules="*",
        sample_size=10000,
        what_if: bool = False,
    ):
        """Scans the lakehouse for columns matching the given rules

        Args:
            from_tables (str, optional): The tables to be scanned in format "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".
            rules (str, optional): The rule names to be used to scan the lakehouse, use "*" as a wildcard. Defaults to "*".
            sample_size (int, optional): The number of rows to be scanned per table. Defaults to 10000.
            what_if (bool, optional): Whether to run the scan in what-if mode and print the SQL commands instead of executing them. Defaults to False.
        """
        catalogs, schemas, tables = Msql.validate_from_components(from_tables)

        self.scanner = Scanner(
            self.spark,
            self.rules,
            catalogs=catalogs,
            schemas=schemas,
            tables=tables,
            rule_filter=rules,
            sample_size=sample_size,
            what_if=what_if,
            columns_table_name=self.COLUMNS_TABLE_NAME,
            max_workers=self.MAX_WORKERS,
        )

        self.scanner.scan()
        self._classify(self.classification_threshold)

    def scan_result(self):
        """Returns the scan results as a pandas DataFrame

        Raises:
            Exception: If the scan has not been run
        """
        if self.scanner is None:
            raise Exception("You first need to scan your lakehouse using Scanner.scan()")
        if self.scanner.scan_result is None:
            raise Exception("Your scan did not finish successfully. Please consider rerunning Scanner.scan()")

        return self.scanner.scan_result.df

    def _classify(self, classification_threshold: float):
        """Classifies the columns in the lakehouse

        Args:
            classification_threshold (float): The frequency threshold (0 to 1) above which a column will be classified

        Raises:
            Exception: If the scan has not been run"""

        if self.scanner is None:
            raise Exception("You first need to scan your lakehouse using Scanner.scan()")
        if self.scanner.scan_result is None:
            raise Exception("Your scan did not finish successfully. Please consider rerunning Scanner.scan()")

        self.classifier = Classifier(
            classification_threshold,
            self.scanner,
            self.spark,
            self.classification_table_name,
        )

        self.logger.friendlyHTML(self.classifier.summary_html)

    def save(self, full_table_name: str = None):
        """Saves the classification results to the lakehouse

        Args:
            full_table_name (str, optional): The full table name to be used to save the classification results. Defaults to None.
        Raises:
            Exception: If the classification has not been run

        """

        # save classes
        self.classifier.save()

    def search(self, search_term: str, from_tables: str = "*.*.*", by_class: Optional[str] = None):
        """Searches your lakehouse for columns matching the given search term

        Args:
            search_term (str): The search term to be used to search for columns.
            from_tables (str, optional): The tables to be searched in format "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_class (str, optional): The class to be used to search for columns. Defaults to None.

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
            self.logger.friendly(
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
            self.logger.friendly(f"Discoverx will search your lakehouse using the class {by_class}")
        elif isinstance(by_class, str):
            search_matching_rules = [by_class]
        else:
            raise ValueError(f"The provided by_class {by_class} must be of string type.")

        sql_filter = f"[{search_matching_rules[0]}] = '{search_term}'"
        select_statement = (
            "named_struct("
            + ", ".join(
                [
                    f"'{rule_name}', named_struct('column', '[{rule_name}]', 'value', [{rule_name}])"
                    for rule_name in search_matching_rules
                ]
            )
            + ") AS search_result"
        )

        if search_term is None:
            where_statement = ""
        else:
            where_statement = f"WHERE {sql_filter}"

        return self._msql(
            f"SELECT {select_statement}, to_json(struct(*)) AS row_content FROM {from_tables} {where_statement}"
        )

    def select_by_classes(self, from_tables: str = "*.*.*", by_classes: Optional[Union[List[str], str]] = None):
        """Selects all columns in the lakehouse from tables that match ALL the given classes

        Args:
            from_tables (str, optional): The tables to be selected in format "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_classes (Union[List[str], str], optional): The classes to be used to search for columns. Defaults to None.

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
                    f"'{class_name}', named_struct('column', '[{class_name}]', 'value', [{class_name}])"
                    for class_name in by_classes
                ]
            )
            + ") AS classified_columns"
        )

        return self._msql(f"SELECT {from_statement}, to_json(struct(*)) AS row_content FROM {from_tables}")

    def delete_by_class(
        self,
        from_tables="*.*.*",
        by_class: str = None,
        values: Optional[Union[List[str], str]] = None,
        yes_i_am_sure: bool = False,
    ):
        """Deletes all rows in the lakehouse that match any of the provided values in a column classified with the given class

        Args:
            from_tables (str, optional): The tables to delete from in format "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_class (str, optional): The class to be used to search for columns. Defaults to None.
            values (Union[List[str], str], optional): The values to be deleted. Defaults to None.
            yes_i_am_sure (bool, optional): Whether you are sure that you want to delete the data. If False prints the SQL statements instead of executing them. Defaults to False.

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
            self.logger.friendly(
                f"Please confirm that you want to delete the following values from the table {from_tables} using the class {by_class}: {values}"
            )
            self.logger.friendly(
                f"If you are sure, please run the same command again but set the parameter yes_i_am_sure to True."
            )

        delete_result = self._msql(
            f"DELETE FROM {from_tables} WHERE [{by_class}] IN ({value_string})", what_if=(not yes_i_am_sure)
        )

        if delete_result is not None:
            self.logger.friendlyHTML(f"<p>The affcted tables are</p>{delete_result.toPandas().to_html()}")

    def _msql(self, msql: str, what_if: bool = False):

        self.logger.debug(f"Executing sql template: {msql}")

        msql_builder = Msql(msql)

        # check if classification is available
        # Check for more specific exception
        try:
            classification_result_pdf = (
                self.spark.sql(f"SELECT * FROM {self.classification_table_name}")
                .filter(func.col("current") == True)
                .select(
                    func.col("table_catalog").alias("catalog"),
                    func.col("table_schema").alias("schema"),
                    func.col("table_name").alias("table"),
                    func.col("column_name").alias("column"),
                    "class_name",
                )
                .toPandas()
            )
        except Exception:
            raise Exception("Classification is not available")

        sql_rows = msql_builder.build(classification_result_pdf)

        if what_if:
            self.logger.friendly(f"SQL that would be executed:")

            for sql_row in sql_rows:
                self.logger.friendly(sql_row.sql)

            return None
        else:
            self.logger.debug(f"Executing SQL:\n{sql_rows}")
            return msql_builder.execute_sql_rows(sql_rows, self.spark)

    def _validate_classification_threshold(self, threshold) -> float:
        """Validate that threshold is in interval [0,1]
        Args:
            threshold (float): The threshold value to be validated
        Returns:
            float: The validated threshold value
        """
        if (threshold < 0) or (threshold > 1):
            error_msg = f"classification_threshold has to be in interval [0,1]. Given value is {threshold}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        return threshold
