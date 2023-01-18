from discoverx.common import Task
from discoverx.data_model import DataModel

class Explorer(Task):
    def __init__(self, spark=None, init_conf=None, date_model: DataModel = None):
        super().__init__(spark, init_conf)

    def launch(self):
        self.logger.info("Launching rule mathcing task")
        
        # Get table info from information schema
        # table_info = data_model.information_schema.get_table_info(self.spark, "default", "sklearn_housing")
        # Get rules
        # Build SQL
        # Execute SQL
        # Write to Delta



        self.spark.sql("CREATE TABLE IF NOT EXISTS default.sklearn_housing (value INT)")
        self.spark.sql("INSERT INTO default.sklearn_housing VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)")

        # self.logger.info("Rule matching task finished!")
