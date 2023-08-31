import polars as pl
from .integers import downcast_integers as downcast_integers
from .strings import downcast_strings as downcast_strings
from typing import TypeVar

DataFrameType = TypeVar('DataFrameType', pl.LazyFrame, pl.DataFrame)

def compress_frame(frame: DataFrameType) -> DataFrameType: ...
def real_test_compresss_file() -> None: ...
