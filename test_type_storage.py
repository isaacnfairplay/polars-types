import os
import polars as pl

# Integer DataFrame
df_int = pl.DataFrame({
    'A': [1] * 1000000  # Integer data
})
df_int.write_parquet('int.parquet')

# String DataFrame
df_str = pl.DataFrame({
    'B': ['a'] * 1000000  # String data
})
df_str.write_parquet('str.parquet')

# Category DataFrame
df_cat = df_str.with_columns(pl.col("B").cast(pl.Categorical))
df_cat.write_parquet('cat.parquet')

# String DataFrame with Additional Literal
df_str_lit = df_str.with_columns(pl.lit(3.14).alias('C'))
print(df_str_lit.schema)
df_str_lit.write_parquet('str_lit.parquet')

# Get file sizes and calculate size per row
dataframes = ['int', 'str', 'cat', 'str_lit']
for df in dataframes:
    size = os.path.getsize(f"{df}.parquet")
    size_per_row = size / 1000000  # as each dataframe has 1,000,000 rows
    print(f"{df}: {size_per_row} bytes per row for {pl.read_parquet(f'{df}.parquet').schema} ")
