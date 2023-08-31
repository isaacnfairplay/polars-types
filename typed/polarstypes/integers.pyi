import polars as pl
from _typeshed import Incomplete
from typing import NamedTuple, TypeVar

DataFrameType = TypeVar('DataFrameType', pl.LazyFrame, pl.DataFrame)

class IntegerRange(NamedTuple):
    datatype: Incomplete
    min_value: Incomplete
    max_value: Incomplete
    bit_resolution: Incomplete

def calculate_int_range(datatype: pl.DataType) -> IntegerRange: ...

TYPE_RANGES: Incomplete

def downcast_integers(frame: DataFrameType) -> DataFrameType: ...
def test_downcast_integers(datatype, mode: str = ...): ...
def test_downcast_integers_all() -> None: ...
