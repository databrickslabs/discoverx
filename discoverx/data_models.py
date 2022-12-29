from dataclasses import dataclass

@dataclass
class Rule:
    name: str
    type: str
    description: str
    definition: str

@dataclass
class ColumnInfo:
    name: str
    data_type: str
    is_partition: bool

@dataclass
class TableInfo:
    metastore: str
    database: str
    table: str
    columns: list[ColumnInfo]
