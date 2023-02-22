from dataclasses import dataclass

# TODO: Add pydantic
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
    partition_index: int


@dataclass
class TableInfo:
    catalog: str
    database: str
    table: str
    columns: list[ColumnInfo]
