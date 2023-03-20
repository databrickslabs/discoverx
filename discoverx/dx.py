import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from typing import List, Optional, Union
from discoverx import logging
from discoverx.common.helper import strip_margin
from discoverx.msql import Msql
from discoverx.rules import Rules, Rule
from discoverx.scanner import Scanner, Classifier
from functools import reduce


class DX:
    """DiscoverX scans and searches your lakehouse
    DiscoverX scans your data for patterns which have been pre-defined
    as rules. You can either use standard rules which come with
    DiscoverX or define and add custom rules.
    Attributes:
        custom_rules (List[Rule], Optional): Custom rules which will be
            used to detect columns with corresponding patterns in your
            data
        column_type_classification_threshold (float, optional):
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
        column_type_classification_threshold: float = 0.95,
        spark: Optional[SparkSession] = None,
    ):

        if spark is None:
            spark = SparkSession.getActiveSession()
        self.spark = spark
        self.logger = logging.Logging()

        self.rules = Rules(custom_rules=custom_rules)
        self.column_type_classification_threshold = self._validate_classification_threshold(
            column_type_classification_threshold
        )

        self.uc_enabled = self.spark.conf.get("spark.databricks.unityCatalog.enabled", "false")

        self.scanner: Optional[Scanner] = None
        self.classifier: Optional[Classifier] = None

        self.intro()

    def intro(self):
        # TODO: Decide on how to do the introduction
        intro_text = """
        <h1>Hi there, I'm DiscoverX.</h1>

        <p>
          I'm here to help you discover data that matches a set of rules in your lakehouse.<br />
          You can scan a sample of 10000 rows per table from your entire lakehouse by using
        </p>
        <pre><code>dx.scan()</code></pre>
        <p>
          For more detailed instructions, use
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

    def help(self):
        snippet1 = strip_margin(
            """
          dx.help()  # This will show you this help message

          dx.intro() # This will show you a short introduction to me

          dx.display_rules() # This will show you the rules that are available to you

          dx.scan()  # This will scan your lakehouse for data that matches a set of rules
        """
        )

        snippet2 = strip_margin(
            """
          dx.scan(output_table="default.discoverx_results")     # Saves the results in 'discoverx_results' table

          dx.scan(catalogs="*", databases="prod_*", tables="*") # Only scans in databases that start with `prod_`

          dx.scan(databases='prod_*', rules=['phone_number'])   # Only scans for phone numbers in databases that start with `prod_`

          dx.scan(sample_size=100)                              # Samples only 100 rows per table

          dx.scan(sample_size=None)                             # Scan each table for the entire content
        """
        )

        text = f"""
        <h2>I'm glad you asked for help.</h2> 
        <p>
          Here are some things you can do with me:
        </p>
        <pre><code>{snippet1}</code></pre>

        <p>
          Examples of dx.scan() usage: 
        </p>
            
        <pre><code>{snippet2}</code></pre>
        """
        self.logger.friendlyHTML(text)

    def display_rules(self):
        text = self.rules.get_rules_info()
        self.logger.friendlyHTML(text)

    def scan(self, catalogs="*", databases="*", tables="*", rules="*", sample_size=10000, what_if: bool = False):
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
        self.classify(self.column_type_classification_threshold)

    def classify(self, column_type_classification_threshold: float):
        if self.scanner is None:
            raise Exception("You first need to scan your lakehouse using Scanner.scan()")
        if self.scanner.scan_result is None:
            raise Exception("Your scan did not finish successfully. Please consider rerunning Scanner.scan()")

        self.classifier = Classifier(column_type_classification_threshold, self.scanner.scan_result)

        self.logger.friendlyHTML(self.classifier.summary_html)

    def search(self,
               search_term: Optional[str] = None,
               search_tags: Optional[Union[List[str], str]] = None,
               catalog: str = "*",
               database: str = "*",
               table: str = "*",
               ):

        if (search_term is None) and (search_tags is None):
            raise ValueError("Neither search_term nor search_tags have been provided. At least one of them need to be specified.")

        if (search_term is not None) and (not isinstance(search_term, str)):
            raise ValueError(f"The search_term type {type(search_term)} is not valid. Either None or a string have to be provided.")

        if search_tags is None:
            self.logger.friendly("You did not provide any tags to be searched. "
                                 "We will try to auto-detect matching rules for the given search term")
            search_matching_rules = self.rules.match_search_term(search_term)
            self.logger.friendly(f"Discoverx will search your lakehouse using the tags {search_matching_rules}")
        elif isinstance(search_tags, str):
            search_matching_rules = [search_tags]
        elif isinstance(search_tags, list) and all(isinstance(elem, str) for elem in search_tags):
            search_matching_rules = search_tags
        else:
            raise ValueError(f"The provided search_tags {search_tags} have the wrong type. Please provide"
                             f" either a str or List[str].")

        sql_filter = [f"[{rule_name}] = '{search_term}'" for rule_name in search_matching_rules]
        from_statement = ', '.join([f'[{rule}] AS {rule}' for rule in search_matching_rules])
        namespace_statement = ".".join([catalog, database, table])
        if search_term is None:
            where_statement = ""
        else:
            where_statement = f"WHERE {' OR '.join(sql_filter)}"

        return self.msql(f"SELECT {from_statement} FROM {namespace_statement} {where_statement}")

    def msql(self, msql: str, what_if: bool = False):

        if self.scanner.scan_result is None:
            message = "You need to run 'dx.scan()' before you can run 'dx.msql()'"
            self.logger.friendly(message)
            raise Exception(message)

        self.logger.debug(f"Executing msql: {msql}")

        msql_builder = Msql(msql)

        sqls = msql_builder.build(self.classifier)

        if what_if:
            self.logger.friendly(f"SQL that would be executed:")

            for sql in sqls:
                self.logger.friendly(sql)

            return None
        else:
            self.logger.debug(f"Executing SQL:\n{sqls}")
            if len(sqls) == 1:
                return self.spark.sql(sqls[0])
            else:
                results = [self.spark.sql(sql).withColumn("sql", lit(sql)) for sql in sqls]
                return reduce(lambda x, y: x.union(y), results)

    def results(self):
        # TODO: We have to return some results here
        self.logger.friendly("Here are the results:")
        # self.explorer.scan_summary()
        # self.explorer.scan_details()

    def _validate_classification_threshold(self, threshold) -> float:
        """Validate that threshold is in interval [0,1]
        Args:
            threshold (float): The threshold value to be validated
        Returns:
            float: The validated threshold value
        """
        if (threshold < 0) or (threshold > 1):
            error_msg = f"column_type_classification_threshold has to be in interval [0,1]. Given value is {threshold}"
            self.logger.error(error_msg)
            raise ValueError(error_msg)
        return threshold
