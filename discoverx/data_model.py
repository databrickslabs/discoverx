from discoverx.config import TableInfo
import fnmatch
class DataModel:
    def __init__(self, logger=None):
        
        if logger is None:
            from discoverx.logging import Logging
            logger = Logging()
        self.logger = logger

    def _apply_filter(self, item: TableInfo, catalogs_filter, databases_filter, tables_filter):
        return fnmatch(item.catalog, catalogs_filter) and fnmatch(item.database, databases_filter) and fnmatch(item.table, tables_filter)

    def get_table_list(self, catalogs_filter, databases_filter, tables_filter):
        table_info = [
            TableInfo("default", "sklearn_housing", "sklearn_housing", []),
            TableInfo("default", "sklearn_housing", "sklearn_housing_2", []),
        ]
        filtered_tables = [x for x in table_info if self._apply_filter(x, catalogs_filter, databases_filter, tables_filter)]

        return filtered_tables
