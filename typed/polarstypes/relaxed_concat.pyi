import polars as pl
from typing import Iterable, TypeVar

DataFrameType = TypeVar('DataFrameType', pl.LazyFrame, pl.DataFrame)

def add_missing_columns(frames: Iterable[DataFrameType]) -> list[DataFrameType]: ...
def majority_schema_concat(frames: Iterable[DataFrameType]) -> DataFrameType: ...
