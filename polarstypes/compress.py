from typing import TypeVar
import polars as pl
DataFrameType = TypeVar('DataFrameType', pl.LazyFrame, pl.DataFrame)

from .integers import downcast_integers
from .strings import downcast_strings

def compress_frame(frame: DataFrameType) -> DataFrameType:
    categorized = downcast_strings(frame)
    integer_bit_shortened = downcast_integers(categorized)
    return integer_bit_shortened

def real_test_compresss_file():
    data_frame = pl.scan_csv("exampledata1.csv", infer_schema_length=1)
    print(f"Original schema: {data_frame.schema}")
    data_frame = compress_frame(data_frame).collect()
    print(f"Schema: {data_frame.schema}")
