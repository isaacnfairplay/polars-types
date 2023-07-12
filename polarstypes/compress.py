import polars as pl
from .integers import downcast_integers
from .strings import downcast_strings

def compress_frame(df):
    df = downcast_strings(df)
    df = downcast_integers(df)
    return df

def real_test_compresss_file():
    df = pl.scan_csv("exampledata1.csv", infer_schema_length=1)
    print(f"Original schema: {df.schema}")
    df = compresss_file(df).collect()
    print(f"Schema: {df.schema}")
