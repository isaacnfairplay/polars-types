import polars as pl

def is_attribute(col, df: pl.LazyFrame):
        return df.select(pl.col(col).unique()).collect().n_unique() == 1

def test_is_attribute():
    test_data = pl.DataFrame({
        'a' : [1, 2, 3],
        'b' : [1, 1, 1] }).lazy()
    assert is_attribute('a', test_data) == False
    assert is_attribute('b', test_data) == True
    print("is_attribute() passed")

test_is_attribute()

def extract_attributes(df: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame | pl.LazyFrame:
    
    start_mode = 'lazy' if isinstance(df, pl.LazyFrame) else 'eager'
    df = df.lazy() if start_mode == 'eager' else df
    static_cols = [col for col in df.columns if is_attribute(col, df)]
    attribute_frame = df.select(static_cols)
    fact_frame = df.select([col for col in df.columns if col not in static_cols])
    attribute_frame = attribute_frame.unique()
    if start_mode == "lazy":
        return attribute_frame, fact_frame
    return attribute_frame.collect(), fact_frame.collect()

def test_extract_attributes():
    test_data = pl.DataFrame({
        'a' : [1, 2, 3],
        'b' : [1, 1, 1] }).lazy()
    attribute_frame, fact_frame = extract_attributes(test_data)
    assert attribute_frame.collect().frame_equal(pl.DataFrame({'b' : [1]})), attribute_frame.collect()
    assert fact_frame.collect().frame_equal(pl.DataFrame({'a' : [1, 2, 3]})), fact_frame.collect()
    print("extract_attributes() passed for lazy frames")
    test_data = test_data.collect()
    attribute_frame, fact_frame = extract_attributes(test_data)
    assert attribute_frame.frame_equal(pl.DataFrame({'b' : [1]})), attribute_frame
    assert fact_frame.frame_equal(pl.DataFrame({'a' : [1, 2, 3]})), fact_frame
    print("extract_attributes() passed for eager frames")

test_extract_attributes()
    
    
