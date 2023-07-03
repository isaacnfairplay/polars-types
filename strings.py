import polars as pl


def downcast_strings(df: pl.DataFrame | pl.LazyFrame) -> pl.DataFrame | pl.LazyFrame:
    original_schema = df.schema


    transform_cols = [col for col in df.columns if original_schema[col] == pl.Utf8]
    if len(transform_cols) == 0:
        return df
    
    transformed_df = df.with_columns([pl.col(col).cast(pl.Categorical) for col in df.columns if original_schema[col] == pl.Utf8])

    return transformed_df
            

def test_downcast_strings_eager():
    df = pl.DataFrame({
        "a": ["a", "b", "c"],
        "b": [1, 2, 3],
        "c": [True, False, True]
    })

    assert df.schema["a"] == pl.Utf8
    assert df.schema["b"] == pl.Int64
    assert df.schema["c"] == pl.Boolean

    df = downcast_strings(df)

    assert df.schema["a"] == pl.Categorical, f"Expected {df.schema['a']} to be {pl.Categorical}"
    assert df.schema["b"] == pl.Int64,f"Expected {df.schema['b']} to be {pl.Int64}"
    assert df.schema["c"] == pl.Boolean,f"Expected {df.schema['c']} to be {pl.Boolean}"

def test_downcast_strings_lazy():
    df = pl.DataFrame({
        "a": ["a", "b", "c"],
        "b": [1, 2, 3],
        "c": [True, False, True]
    }).lazy()

    assert df.schema["a"] == pl.Utf8
    assert df.schema["b"] == pl.Int64
    assert df.schema["c"] == pl.Boolean

    df = downcast_strings(df)

    df = df.collect()
    assert df.schema["a"] == pl.Categorical, f"Expected {df.schema['a']} to be {pl.Categorical}"
    assert df.schema["b"] == pl.Int64,f"Expected {df.schema['b']} to be {pl.Int64}"
    assert df.schema["c"] == pl.Boolean,f"Expected {df.schema['c']} to be {pl.Boolean}"
