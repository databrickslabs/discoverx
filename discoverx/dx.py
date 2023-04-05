from functools import wraps
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from typing import List, Optional, Union
from discoverx import logging
from discoverx.common.helper import strip_margin
from discoverx.msql import Msql
from discoverx.rules import Rules, Rule
from discoverx.scanner import Scanner
from discoverx.classification import Classifier
from discoverx.inspection import InspectionTool
import ipywidgets as widgets


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
    """

    def __init__(
        self,
        custom_rules: Optional[List[Rule]] = None,
        classification_threshold: float = 0.95,
        spark: Optional[SparkSession] = None,
        classification_table_name: str = "_discoverx.classification.tags",
    ):

        if spark is None:
            spark = SparkSession.getActiveSession()
        self.spark = spark
        self.logger = logging.Logging()

        self.rules = Rules(custom_rules=custom_rules)

        self.classification_threshold = (
            self._validate_classification_threshold(
                classification_threshold
            )
        )
        self.classification_table_name = classification_table_name

        self.uc_enabled = self.spark.conf.get(
            "spark.databricks.unityCatalog.enabled", "false"
        )

        self.scanner: Optional[Scanner] = None
        self.classifier: Optional[Classifier] = None

        self.intro()
        self.out = widgets.Output()

    def intro(self):
        # TODO: Decide on how to do the introduction
        intro_text = """
        <h1>Hi there, I'm DiscoverX.</h1>

        <p>
          I'm here to help you discover data in your lakehouse.<br />
          You can scan your lakehouse by using
        </p>
        <pre><code>dx.scan()</code></pre>
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

        if self.uc_enabled == "true":
            self.logger.friendlyHTML(intro_text)
        else:
            self.logger.friendlyHTML(missing_uc_text)


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
            from_tables (str, optional): The tables to be scanned in format "catalog.database.table", use "*" as a wildcard. Defaults to "*.*.*".
            rules (str, optional): The rule names to be used to scan the lakehouse, use "*" as a wildcard. Defaults to "*".
            sample_size (int, optional): The number of rows to be scanned per table. Defaults to 10000.
            what_if (bool, optional): Whether to run the scan in what-if mode and print the SQL commands instead of executing them. Defaults to False.
        """
        catalogs, databases, tables = Msql.validate_from_components(from_tables)
        
        self.scanner = Scanner(
            self.spark,
            self.rules,
            catalogs=catalogs,
            databases=databases,
            tables=tables,
            rule_filter=rules,
            sample_size=sample_size,
            what_if=what_if,
        )

        self.scanner.scan()
        self._classify(self.classification_threshold)

    def _classify(self, classification_threshold: float):
        """Classifies the columns in the lakehouse
        
        Args:
            classification_threshold (float): The frequency threshold (0 to 1) above which a column will be classified
            
        Raises:
            Exception: If the scan has not been run"""
        
        if self.scanner is None:
            raise Exception(
                "You first need to scan your lakehouse using Scanner.scan()"
            )
        if self.scanner.scan_result is None:
            raise Exception(
                "Your scan did not finish successfully. Please consider rerunning Scanner.scan()"
            )


        self.classifier = Classifier(
            classification_threshold,
            self.scanner,
            self.spark,
            self.classification_table_name,
        )

        self.logger.friendlyHTML(self.classifier.summary_html)

    def inspect(self):
        """Displays the inspection tool for the classification results
        
        Raises:
            Exception: If the classification has not been run
        """
        # until we have an end-2-end interactive UI we need to 
        # rerun classification to make sure users can rerun inspect
        # without rerunning the scan
        self.classifier.compute_classification_result()
        self.classifier.inspect()
        self.classifier.inspection_tool.display()

    def publish(self, publish_uc_tags=False):
        """Publishes the classification results to the lakehouse 
        
        Args:
            publish_uc_tags (bool, optional): Whether to publish the tags to Unity Catalog. Defaults to False.
        
        Raises:
            Exception: If the classification has not been run
        
        """
        
        # save tags
        self.classifier.publish(publish_uc_tags=publish_uc_tags)

    def search(self,
               search_term: Optional[str] = None,
               from_tables: str = "*.*.*",
               by_tags: Optional[Union[List[str], str]] = None
               ):
        """Searches your lakehouse for columns matching the given search term
        
        Args:
            search_term (str, optional): The search term to be used to search for columns. Defaults to None.
            from_tables (str, optional): The tables to be searched in format "catalog.database.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_tags (Union[List[str], str], optional): The tags to be used to search for columns. Defaults to None.
            
        Raises:
            ValueError: If neither search_term nor by_tags have been provided
            ValueError: If the search_term type is not valid
            ValueError: If the by_tags type is not valid
        
        Returns:
            DataFrame: A dataframe containing the results of the search
        """
        
        Msql.validate_from_components(from_tables)

        if (search_term is None) and (by_tags is None):
            raise ValueError("Neither search_term nor by_tags have been provided. At least one of them need to be specified.")

        if (search_term is not None) and (not isinstance(search_term, str)):
            raise ValueError(f"The search_term type {type(search_term)} is not valid. Either None or a string have to be provided.")

        if by_tags is None:
            self.logger.friendly("You did not provide any tags to be searched. "
                                 "We will try to auto-detect matching rules for the given search term")
            search_matching_rules = self.rules.match_search_term(search_term)
            self.logger.friendly(f"Discoverx will search your lakehouse using the tags {search_matching_rules}")
        elif isinstance(by_tags, str):
            search_matching_rules = [by_tags]
        elif isinstance(by_tags, list) and all(isinstance(elem, str) for elem in by_tags):
            search_matching_rules = by_tags
        else:
            raise ValueError(f"The provided by_tags {by_tags} have the wrong type. Please provide"
                             f" either a str or List[str].")

        sql_filter = [f"[{rule_name}] = '{search_term}'" for rule_name in search_matching_rules]
        select_statement = "named_struct(" + ', '.join([f"'{rule_name}', named_struct('column', '[{rule_name}]', 'value', [{rule_name}])" for rule_name in search_matching_rules]) + ") AS search_result"
        
        if search_term is None:
            where_statement = ""
        else:
            where_statement = f"WHERE {' OR '.join(sql_filter)}"

        return self._msql(f"SELECT {select_statement}, to_json(struct(*)) AS row_content FROM {from_tables} {where_statement}")

    def select_by_tags(self,
            from_tables: str = "*.*.*",
            by_tags: Optional[Union[List[str], str]] = None):
        """Selects all columns in the lakehouse that match the given tags
        
        Args:
            from_tables (str, optional): The tables to be selected in format "catalog.database.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_tags (Union[List[str], str], optional): The tags to be used to search for columns. Defaults to None.
        
        Raises:
            ValueError: If the by_tags type is not valid
        
        Returns:
            DataFrame: A dataframe containing the UNION ALL results of the select"""

        Msql.validate_from_components(from_tables)

        if isinstance(by_tags, str):
            by_tags = [by_tags]
        elif isinstance(by_tags, list) and all(isinstance(elem, str) for elem in by_tags):
            by_tags = by_tags
        else:
            raise ValueError(f"The provided by_tags {by_tags} have the wrong type. Please provide"
                             f" either a str or List[str].")

        from_statement = "named_struct(" + ', '.join([f"'{tag}', named_struct('column', '[{tag}]', 'value', [{tag}])" for tag in by_tags]) + ") AS tagged_columns"
        
        return self._msql(f"SELECT {from_statement}, to_json(struct(*)) AS row_content FROM {from_tables}")

    def delete_by_tag(self,
               from_tables = "*.*.*",
               by_tag: str = None,
               values: Optional[Union[List[str], str]] = None,
               yes_i_am_sure: bool = False
               ):
        """Deletes all rows in the lakehouse that match any of the provided values in a column tagged with the given tag
        
        Args:
            from_tables (str, optional): The tables to delete from in format "catalog.database.table", use "*" as a wildcard. Defaults to "*.*.*".
            by_tag (str, optional): The tag to be used to search for columns. Defaults to None.
            values (Union[List[str], str], optional): The values to be deleted. Defaults to None.
            yes_i_am_sure (bool, optional): Whether you are sure that you want to delete the data. If False prints the SQL statements instead of executing them. Defaults to False.
            
        Raises:
            ValueError: If the from_tables is not valid
            ValueError: If the by_tag is not valid
            ValueError: If the values is not valid
        """

        Msql.validate_from_components(from_tables)

        if (by_tag is None) or (not isinstance(by_tag, str)):
            raise ValueError(f"Please provide a tag to identify the columns to be matched on the provided values.")

        if values is None:
            raise ValueError(f"Please specify the values to be deleted. You can either provide a list of values or a single value.")
        elif isinstance(values, str):
            value_string = f"'{values}'"
        elif isinstance(values, list) and all(isinstance(elem, str) for elem in values):
            value_string = "'" + "', '".join(values) + "'"
        else:
            raise ValueError(f"The provided values {values} have the wrong type. Please provide"
                             f" either a str or List[str].")
        
        if not yes_i_am_sure:
            self.logger.friendly(f"Please confirm that you want to delete the following values from the table {from_tables} using the tag {by_tag}: {values}")
            self.logger.friendly(f"If you are sure, please run the same command again but set the parameter yes_i_am_sure to True.")

        return self._msql(f"DELETE FROM {from_tables} WHERE [{by_tag}] IN ({value_string})", what_if=(not yes_i_am_sure))

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
                    func.col("table_schema").alias("database"),
                    func.col("table_name").alias("table"),
                    func.col("column_name").alias("column"),
                    "tag_name",
                ).toPandas()
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
