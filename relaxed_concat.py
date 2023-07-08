import polars as pl


def add_missing_columns(frames: iter[pl.DataFrame]):
    """
    Add columns that are missing from a frame
    """
    columns = set()
    for frame in frames:
        columns.update(frame.columns)
    for frame in frames:
        missing_columns = columns - set(frame.columns)
        frame = frame.with_columns([pl.lit(None).alias(column) for column in missing_columns])
    return frames


def majority_schema_concat(frames: iter[pl.DataFrame]):
    """
    Concatenate a list of frames into a single frame
    """
    extra_column_frame = add_missing_columns(frames)
    return pl.concat(extra_column_frame, how="vertical_relaxed")
        