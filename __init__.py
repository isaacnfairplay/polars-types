import polars as pl
from integers import downcast_integers
from strings import downcast_strings

def compresss_file(df):
    df = downcast_strings(df)
    df = downcast_integers(df)
    return df

def real_test_compresss_file():
    df = pl.read_csv("https://media.githubusercontent.com/media/datablist/sample-csv-files/main/files/people/people-100000.csv")
    print(f"Original schema: {df.schema}, size: {df.estimated_size()/1024} kb")
    df = compresss_file(df)
    print(f"Schema: {df.schema}, size: {df.estimated_size()/1024} kb")

real_test_compresss_file()