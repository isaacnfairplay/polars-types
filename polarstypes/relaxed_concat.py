from typing import TypeVar, Iterable
import polars as pl
DataFrameType = TypeVar('DataFrameType', pl.LazyFrame, pl.DataFrame)


def add_missing_columns(frames: Iterable[DataFrameType]) -> list[DataFrameType]:
    """
    Add columns that are missing from a frame
    """
    columns = set()
    frames = []
    for frame in frames:
        columns.update(frame.columns)
    for frame in frames:
        missing_columns = columns - set(frame.columns)
        frames.append(frame.with_columns([pl.lit(None).alias(column) for column in missing_columns]))
    return frames


def majority_schema_concat(frames: Iterable[DataFrameType]) -> DataFrameType:
    """
    Concatenate a list of frames into a single frame
    """
    extra_column_frame = add_missing_columns(frames)
    return pl.concat(extra_column_frame, how="vertical_relaxed")
        