import pandas as pd
from pyspark.sql import SparkSession
from typing import List, Optional
from discoverx import logging
from discoverx.explorer import DataExplorer, InfoFetcher


class DX:
    """DiscoverX offers a set of tools to facilitate lakehouse administration.

    Attributes:
        spark (SparkSession, optional): The SparkSession which will be
            used to scan your data. Defaults to None.
    """

    INFORMATION_SCHEMA = "system.information_schema"
    MAX_WORKERS = 10

    def __init__(
        self,
        spark: Optional[SparkSession] = None,
    ):
        if spark is None:
            spark = SparkSession.getActiveSession()
        self.spark = spark
        self.logger = logging.Logging()

        self.uc_enabled = self.spark.conf.get("spark.databricks.unityCatalog.enabled", "false") == "true"

        self.intro()

    def _can_read_columns_table(self) -> bool:
        try:
            self.spark.sql(f"SELECT * FROM {self.INFORMATION_SCHEMA}.columns WHERE table_catalog = 'system' LIMIT 1")
            return True
        except Exception as e:
            self.logger.error(f"Error while reading table {self.INFORMATION_SCHEMA}.columns: {e}")
            return False

    def intro(self):
        intro_text = """
        <h1>Hi there, I'm DiscoverX.</h1>

        <p>
          I'm here to help you with lakehouse administration and management.<br />
          You can start by defining the set of tables to run operations on (use "*" as a wildcard)<br />
        </p>
        <pre><code>dx.from_tables("*.*.*")</code></pre>
        <p>
            Then you can apply the following operations
            <ul>
                <li><code>.with_sql(...)</code> - Runs a SQL template on each table</li>
                <li><code>.map(...)</code> - Runs a python function on each table</li>
                <li><code>.classify_columns(...)</code> - Scan your lakehouse for columns matching the given rules</li>
            </ul>
        </p>
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
        <h1 style="color: red">DiscoverX needs access to the system tables `system.information_schema.*`</h1>
        
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
        if not self._can_read_columns_table():
            self.logger.friendlyHTML(missing_access_to_columns_table_text)
        else:
            self.logger.friendlyHTML(intro_text)

    def from_tables(self, from_tables: str = "*.*.*"):
        """Returns a DataExplorer object for the given tables

        Args:
            from_tables (str, optional): The tables to be selected in format
                "catalog.schema.table", use "*" as a wildcard. Defaults to "*.*.*".

        Raises:
            ValueError: If the from_tables is not valid

        Returns:
            DataExplorer: A DataExplorer object for the given tables

        """

        return DataExplorer(from_tables, self.spark, InfoFetcher(self.spark, self.INFORMATION_SCHEMA))
