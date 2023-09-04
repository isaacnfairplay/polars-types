import polars as pl
from typing import TypeVar, Generator, Sequence, Set

def recursive_pivot_iter(frame: pl.DataFrame,
        values: str,
        index: str | Sequence[str] | None,
        columns: str,
        maintain_order: bool = True,
        sort_columns: bool = False,
        seperator: str = "_")-> Generator[pl.DataFrame, None, None]:

    current_frame = frame
    while True:
        new_frame_cols = current_frame.select(columns).unique().to_series().to_list()
        new_frame = current_frame.pivot(values=values,index=index,columns=columns, maintain_order=maintain_order,sort_columns=sort_columns,separator=seperator, aggregate_function='min')
        if 0 in new_frame.shape:
            break
        yield new_frame
        print(new_frame.columns)
        max_value_frame = new_frame.select(new_frame_cols).max()
        max_stamp_overall = max_value_frame.transpose().max().item()
        current_frame = current_frame.filter(pl.col(values) > max_stamp_overall)

def recursive_pivot_concat(frame: pl.DataFrame,
        values: str,
        index: str | Sequence[str] | None,
        columns: str,
        maintain_order: bool = True,
        sort_columns: bool = False,
        separator: str = "_") -> pl.DataFrame:
        unique_values = list(map(str,frame.select(columns).unique(maintain_order=True).to_series().to_list()))
        frames = list(recursive_pivot_iter(frame, values, index, columns, maintain_order, sort_columns, separator))
        all_cols_ordered = [col for col in frame.columns if col != values and col!=columns] + unique_values
        frames_with_missing = [frame.with_columns([pl.lit(None).alias(col) for col in all_cols_ordered if col not in frame.columns]).select(all_cols_ordered) for frame in frames]
        return pl.concat(frames_with_missing, how='vertical_relaxed')



# Test for recursive_pivot_iter and recursive_pivot_concat
def test_recursive_pivot():
    # Initialize DataFrame with timestamps and garages
    df = pl.DataFrame({
        "Timestamp": [1, 2, 3, 4, 5, 6, 7],
        "Garage": ["A", "A", "B", "A", "B", "C", "C"],
        "Value": [10, 10, 10, 10, 10, 10, 10]
    })

    # Use recursive_pivot_iter function
    result = recursive_pivot_concat(df, index="Value", columns="Garage", values="Timestamp")
    # There should be 3 unique DataFrames since each garage has different min timestamps
    """ 
    ┌───────┬──────┬──────┬─────┐
    │ Value ┆ A    ┆ B    ┆ C   │
    │ ---   ┆ ---  ┆ ---  ┆ --- │
    │ i64   ┆ i64  ┆ i64  ┆ i64 │
    ╞═══════╪══════╪══════╪═════╡
    │ 10    ┆ 1    ┆ 3    ┆ 6   │
    │ 10    ┆ null ┆ null ┆ 7   │
    └───────┴──────┴──────┴─────┘"""
    print(result)
    expected_frame = pl.DataFrame({
        "Value": [10, 10],
        "A": [1, None],
        "B": [3, None],
        "C": [6, 7]})
       
    assert result.frame_equal(expected_frame)


# Run the test
#test_recursive_pivot()
