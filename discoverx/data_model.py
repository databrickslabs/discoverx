from discoverx.config import TableInfo

class DataModel:
    def __init__(self, logger=None):
        
        if logger is None:
            from discoverx.logging import Logging
            logger = Logging()
        self.logger = logger

    def get_table_list(self, catalogs_filter, databases_filter, tables_filter):
        return [
            TableInfo("default", "sklearn_housing", "sklearn_housing", []),
            TableInfo("default", "sklearn_housing", "sklearn_housing_2", []),
        ]
