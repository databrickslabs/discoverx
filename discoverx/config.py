from dataclasses import dataclass
from typing import Optional

@dataclass
class ColumnInfo:
    name: str
    data_type: str
    partition_index: int
    tags: list[str]


@dataclass
class TableInfo:
    catalog: Optional[str]
    database: str
    table: str
    columns: list[ColumnInfo]

    def get_columns_by_tag(self, tag: str):
        return [col.name for col in self.columns if tag in col.tags]
