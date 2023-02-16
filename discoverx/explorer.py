from discoverx.config import TableInfo, Rule
from discoverx.data_model import DataModel
from discoverx.logging import Logging
from fnmatch import fnmatch

class Explorer:

    def __init__(self, spark=None, init_conf=None, date_model: DataModel = None, logger=None):
        if logger is None:
            logger = Logging()
        self.logger = logger

        if date_model is None:
            date_model = DataModel()
        self.data_model = date_model

    def get_table_list(self, catalogs_filter, databases_filter, tables_filter):
        return self.data_model.get_table_list(catalogs_filter, databases_filter, tables_filter)

    def get_rule_list(self, rules_filter="*"):
        all_rules = [
            Rule("ip_v4", "regex", "IP address v4", r"(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)"),
            Rule("ip_v6", "regex", "IP address v6", r"(([0-9a-fA-F]{1,4}:){7,7}[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,7}:|([0-9a-fA-F]{1,4}:){1,6}:[0-9a-fA-F]{1,4}|([0-9a-fA-F]{1,4}:){1,5}(:[0-9a-fA-F]{1,4}){1,2}|([0-9a-fA-F]{1,4}:){1,4}(:[0-9a-fA-F]{1,4}){1,3}|([0-9a-fA-F]{1,4}:){1,3}(:[0-9a-fA-F]{1,4}){1,4}|([0-9a-fA-F]{1,4}:){1,2}(:[0-9a-fA-F]{1,4}){1,5}|[0-9a-fA-F]{1,4}:((:[0-9a-fA-F]{1,4}){1,6})|:((:[0-9a-fA-F]{1,4}){1,7}|:)|fe80:(:[0-9a-fA-F]{0,4}){0,4}%[0-9a-zA-Z]{1,}|::(ffff(:0{1,4}){0,1}:){0,1}((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])|([0-9a-fA-F]{1,4}:){1,4}:((25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9])\.){3,3}(25[0-5]|(2[0-4]|1{0,1}[0-9]){0,1}[0-9]))"),
        ]
        filtered_rules = [x for x in all_rules if fnmatch(x.name, rules_filter)]
        
        return filtered_rules

    def scan(self, table_list: list[TableInfo], rule_list, sample_size):
        self.logger.debug("Launching lakehouse scanning task\n")
        
        n_tables = len(table_list)
        import time
        for i, table in enumerate(table_list):
            time.sleep(3)
            self.logger.friendly(
                f"Scanning table '{table.catalog}.{table.database}.{table.table}' ({i + 1}/{n_tables})"
            )

        self.logger.debug("Finished lakehouse scanning task")

        # Get table info from information schema
        # table_info = data_model.information_schema.get_table_info(self.spark, "default", "sklearn_housing")
        # Get rules
        # Build SQL
        # Execute SQL
        # Write to Delta



        # self.spark.sql("CREATE TABLE IF NOT EXISTS default.sklearn_housing (value INT)")
        # self.spark.sql("INSERT INTO default.sklearn_housing VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")

        # self.logger.info("Rule matching task finished!")
