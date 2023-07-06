# this module is focused on identifying and extracting static columns

import polars as pl

def identify_static_cols(df: pl.DataFrame | pl.LazyFrame)-> list[str]:
    """
    Identify columns that are static attributes
    """
    # loop through df.columns and identify columns that are static
    static_cols = []
    for col in df.columns:
        #This should work for lazy and eager frames
        column_frame = df.select(pl.col(col).n_unique())
        if not isinstance(df, pl.DataFrame):
            column_frame = column_frame.collect()

        if column_frame[col].to_list()[0] == 1:
            static_cols.append(col)
    return static_cols

def remove_static_cols(df: pl.DataFrame | pl.LazyFrame)-> dict[str, pl.DataFrame | pl.LazyFrame, pl.DataFrame | pl.LazyFrame]:
    """
    Remove columns that are static attributes
    """
    static_cols = identify_static_cols(df)
    attr_df = df.select(static_cols)
    attr_df = attr_df.select(pl.all().unique())
    if not isinstance(attr_df, pl.DataFrame):
        attr_df = attr_df.collect()
    
    # get the static values of each of the attr_df columns
    #use select to get all the values of each column
    
    static_values_lists = [attr_df.select(col)[col].to_list()  for col in attr_df.columns]

    if {col for col in static_values if len(set(col)) != 1}:
        raise ValueError("Static columns with unique values are not all static - see {attr_df.distinct()}")
    
    static_values = [static_values[col][0] for col in static_values_lists]

    attributes = pl.DataFrame({
        'column_name': 
        static_cols, 
        'value': 
            static_values,
        'data_type':
            [attr_df.schema[col] for col in static_cols]
            })
    
    data = df.select([col for col in df.columns if col not in static_cols])
    
    return {"data": data, "attributes": attributes}
        
def test_identify_static_cols():
    test_frame = pl.DataFrame({
        "a": [1,1,1,1,1,1,1,1,1,1], 
        "b": [1,1,1,1,1,1,1,1,1,1],
        "c": [1,1,1,1,1,1,1,1,1,1],
        'x': [1,2,3,4,5,6,7,8,9,10],
        #string columns
        "d": ["a","a","a","a","a","a","a","a","a","a"],
        "e": ["a","a","a","a","a","a","a","a","a","a"],
        "ecat": ["a","a","a","a","a","a","a","a","a","a"],
        #float columns
        "f": [1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0],
        "g": [1.0,0.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0,1.0],
        #bool columns
        'h': [True, True, True, True, True, True, True, True, True, True],
        'i': [True, True, True, True, True, True, True, True, True, True],
        'j': [True, True, True, False, True, True, True, True, True, True]
    })
    # Convert one string column to categorical
    test_frame = test_frame.with_columns(pl.col("ecat").cast(pl.Categorical))
    #Test function
    test_values = ('a', 'b', 'c', 'd', 'e', 'ecat', 'f', 'h', 'i')
    static_cols = identify_static_cols(test_frame)
    missing = {col for col in test_values if col not in static_cols}
    added = {col for col in static_cols if col not in test_values}
    assert tuple(static_cols) == test_values, f"static_cols missing{missing} added{added}"
    #Test function with lazy frame'
    test_frame = test_frame.lazy()
    static_cols = identify_static_cols(test_frame)
    missing = {col for col in test_values if col not in static_cols}
    added = {col for col in static_cols if col not in test_values}
    assert tuple(static_cols) == test_values, f"static_cols missing{missing} added{added}"
    print("Test passed")
        
#make call to test func
test_identify_static_cols()
    