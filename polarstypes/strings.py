from typing import TypeVar
import polars as pl
DataFrameType = TypeVar('DataFrameType', pl.LazyFrame, pl.DataFrame)


def downcast_strings(data_frame: DataFrameType) -> DataFrameType:
    original_schema = data_frame.schema


    transform_cols = [col for col in data_frame.columns if original_schema[col] == pl.Utf8]
    if len(transform_cols) == 0:
        return data_frame
    
    transformed_data_frame = data_frame.with_columns([pl.col(col).cast(pl.Categorical) for col in data_frame.columns if original_schema[col] == pl.Utf8])

    return transformed_data_frame
            

def test_downcast_strings_eager():
    data_frame = pl.DataFrame({
        "a": ["a", "b", "c"],
        "b": [1, 2, 3],
        "c": [True, False, True]
    })

    assert data_frame.schema["a"] == pl.Utf8
    assert data_frame.schema["b"] == pl.Int64
    assert data_frame.schema["c"] == pl.Boolean

    data_frame = downcast_strings(data_frame)

    assert data_frame.schema["a"] == pl.Categorical, f"Expected {data_frame.schema['a']} to be {pl.Categorical}"
    assert data_frame.schema["b"] == pl.Int64,f"Expected {data_frame.schema['b']} to be {pl.Int64}"
    assert data_frame.schema["c"] == pl.Boolean,f"Expected {data_frame.schema['c']} to be {pl.Boolean}"

def test_downcast_strings_lazy():
    data_frame = pl.DataFrame({
        "a": ["a", "b", "c"],
        "b": [1, 2, 3],
        "c": [True, False, True]
    }).lazy()

    assert data_frame.schema["a"] == pl.Utf8
    assert data_frame.schema["b"] == pl.Int64
    assert data_frame.schema["c"] == pl.Boolean

    data_frame = downcast_strings(data_frame)

    data_frame = data_frame.collect()
    assert data_frame.schema["a"] == pl.Categorical, f"Expected {data_frame.schema['a']} to be {pl.Categorical}"
    assert data_frame.schema["b"] == pl.Int64,f"Expected {data_frame.schema['b']} to be {pl.Int64}"
    assert data_frame.schema["c"] == pl.Boolean,f"Expected {data_frame.schema['c']} to be {pl.Boolean}"
