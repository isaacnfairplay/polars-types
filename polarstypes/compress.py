import polars as pl
from .integers import downcast_integers
from .strings import downcast_strings

def compress_frame(frame: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    categorized = downcast_strings(frame)
    integer_bit_shortened = downcast_integers(categorized)
    return integer_bit_shortened

def real_test_compresss_file():
    df = pl.scan_csv("exampledata1.csv", infer_schema_length=1)
    print(f"Original schema: {df.schema}")
    df = compresss_file(df).collect()
    print(f"Schema: {df.schema}")
