import pandas as pd
from pyspark.sql import SparkSession
from typing import List, Optional
from discoverx import logging
from discoverx.common.helper import strip_margin
from discoverx.rules import Rules, Rule
from discoverx.config import TableInfo
from discoverx.data_model import DataModel
from discoverx.sql_builder import SqlBuilder


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
        data_model: Optional[DataModel] = None,
        spark: Optional[SparkSession] = None,
    ):
        if data_model is None:
            data_model = DataModel()
        self.data_model = data_model

        self.logger = logging.Logging()

        if spark is None:
            spark = SparkSession.getActiveSession()
        self.spark = spark

        self.rules = Rules(custom_rules=custom_rules)
        self.column_type_classification_threshold = self._validate_classification_threshold(
            column_type_classification_threshold
        )
        self.database: Optional[str] = None  # TODO: for later use
        
        self.uc_enabled = self.spark.conf.get('spark.databricks.unityCatalog.enabled', 'false')
        
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
        <pre><code>dx.help()</code></pre>
        """
        
        missing_uc_text = """
        <h1>Uch! DiscoverX needs Unity Catalog to be enabled</h1>

        <p>
          Please make sure you have Unity Catalog enabled, and that you are running a Cluster that supports Unity Catalog.
        </p>
        """
        
        if (self.uc_enabled == 'true'):
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

    def scan(self, catalogs="*", databases="*", tables="*", rules="*", sample_size=10000):

        table_list = self.data_model.get_table_list(catalogs, databases, tables)
        rule_list = self.rules.get_rules(rule_filter=rules)

        n_catalogs = len(set(map(lambda x: x.catalog, table_list)))
        n_databases = len(set(map(lambda x: x.database, table_list)))
        n_tables = len(table_list)
        n_rules = len(rule_list)

        text = f"""
        Ok, I'm going to scan your lakehouse for data that matches your rules.
        This is what you asked for:
        
            catalogs ({n_catalogs}) = {catalogs}
            databases ({n_databases}) = {databases}
            tables ({n_tables}) = {tables}
            rules ({n_rules}) = {rules}
            sample_size = {sample_size}
        
        This may take a while, so please be patient. I'll let you know when I'm done.
        ...
        """
        self.logger.friendly(strip_margin(text))

        self.scan_result = self._execute_scan(table_list, rule_list, sample_size)

        self.logger.friendlyHTML(
            f"""
        <h2>I've finished scanning your data lake.</h2>
        <p>
          Here is a summary of the results:
            # TODO
        </p>
        
        """
        )


    def _execute_scan(self, table_list: list[TableInfo], rule_list: list[Rule], sample_size: int) -> pd.DataFrame:

        self.logger.debug("Launching lakehouse scanning task\n")
        
        n_tables = len(table_list)
        builder = SqlBuilder()
        dfs = []

        for i, table in enumerate(table_list):
            self.logger.friendly(
                f"Scanning table '{table.catalog}.{table.database}.{table.table}' ({i + 1}/{n_tables})"
            )
            
            try:
                # Build rule matching SQL
                sql = builder.rule_matching_sql(table, rule_list, sample_size)

                # Execute SQL and append result
                dfs.append(self.spark.sql(sql).toPandas())
            except Exception as e:
                self.logger.error(f"Error while scanning table '{table.catalog}.{table.database}.{table.table}': {e}")
                continue        

        self.logger.debug("Finished lakehouse scanning task")
        
        if dfs:
          return pd.concat(dfs)
        else:
          return pd.DataFrame()

    def results(self):
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

    def _validate_database(self):
        """Validate that output table exists, otherwise raise error
        """
        if not self.spark.catalog.databaseExists(self.database):
            db_error = f"The given database {self.database} does not exist."
            self.logger.error(db_error)
            raise ValueError(db_error)
