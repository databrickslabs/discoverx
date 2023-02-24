from dataclasses import dataclass


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    partition_index: int


@dataclass
class TableInfo:
    catalog: str
    database: str
    table: str
    columns: list[ColumnInfo]
