from dataclasses import dataclass
from typing import Optional

@dataclass
class ColumnInfo:
    name: str
    data_type: str
    partition_index: int


@dataclass
class TableInfo:
    catalog: Optional[str]
    database: str
    table: str
    columns: list[ColumnInfo]
