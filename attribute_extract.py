# this module is focused on identifying and extracting static columns

import polars as pl

def identify_static_cols(df: pl.DataFrame | pl.LazyFrame)-> list[str]:
    """
    Identify columns that are static attributes
    """
    # loop through df.columns and identify columns that are static
    df =df.lazy()
    return [col for col in df.columns if df.select(pl.col(col).n_unique()).collect()[col].to_list()[0]==1]    

def extract_static_cols(df: pl.DataFrame | pl.LazyFrame)-> dict[str, pl.DataFrame | pl.LazyFrame, pl.DataFrame | pl.LazyFrame]:
    """
    Remove columns that are static attributes
    """
    static_cols = identify_static_cols(df)
    attr_df = df.select(static_cols)
    attributes = attr_df.select(pl.all().unique()) #get the unique values of each column
    if not isinstance(attr_df, pl.DataFrame):
        attributes = attr_df.collect()
    
    # get the static values of each of the attr_df columns
    #use select to get all the values of each column
    data = df.select([col for col in df.columns if col not in static_cols])
    
    return {"data": data, "attributes": attributes}


#TESTS
  
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
def test_extract_static_cols():
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
    static_cols = extract_static_cols(test_frame)
    comparison_attributes = pl.DataFrame({
        "a": [1],
        "b": [1],
        "c": [1],
        "d": ["a"],
        "e": ["a"],
        "ecat": ["a"],
        "f": [1.0],
        "h": [True],
        "i": [True]
    })
    
    comparison_data = test_frame.select([col for col in test_frame.columns if col not in test_values])
    
    assert static_cols["attributes"].frame_equal(comparison_attributes), f"attributes not equal"
    assert static_cols["data"].frame_equal(comparison_data), f"data not equal"
    print("Test passed")
