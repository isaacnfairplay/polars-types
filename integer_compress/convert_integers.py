import polars as pl
from collections import OrderedDict
#Testing
from io import StringIO
import numpy as np

#This dictionary will define the ranges of each singed integer datatype
#The key will be the max /min of the  datatype
signed_integer_ranges = OrderedDict(
    [
        (127, pl.Int8),
        (32767, pl.Int16),
        (2147483647, pl.Int32,),
        (9223372036854775807, pl.Int64),
    ]
)
#now for unsigned integer ranges
#The key will be the max /min of the  datatype
unsigned_integer_ranges = OrderedDict(  
    [
        (256, pl.UInt8),
        (65536, pl.UInt16),
        (4294967296, pl.UInt32),
        (18446744073709551616, pl.UInt64),
    ]

)

def downcast_integers(df):
    
    """ evaluate range of values in column - only do this once to be efficent
    Lets layout a process flow of how we will downcast the integers so we get the order right
    We will first check if the column is an integer
    If the column is an integer, we will check if the column is signed or unsigned
    We will then check the minmax of integer columns
    We know that any columns with min>-1 can be unsigned
    Note: lets use math to create the minmax values for each integer type using powers of 2
    lets first create a mapping of castings using polars syntax and avoid pandas syntax
    """
    original = type(df)
    if original == pl.DataFrame:
        df = df.lazy()
    column_transforms = [] # ex pl.col(colname).cast(selected_dtype), pl.col(colname).cast(selected_dtype))
    schema = df.schema #tuple of (column_name, dtype)

    for column_name in df.columns:
        
        original_dtype = schema[column_name]
        #print(original_dtype)
        #Check if datatype is any integer
        if not any([dtype == original_dtype for dtype in (pl.Int8,
            pl.Int16,
            pl.Int32,
            pl.Int64,
            pl.List,
            pl.UInt8,
            pl.UInt16,
            pl.UInt32,
            pl.UInt64)
            ]):
        #leave column as is if not integer
            continue
        #get min and max integer values and save the results
        min_value = df.select(pl.col(column_name).min()).collect()[column_name][0]
        max_value = df.select(pl.col(column_name).max()).collect()[column_name][0]
        #print(min_value, max_value)
        amplitude = max(abs(min_value), abs(max_value))

        #check if column is signed or unsigned
        if min_value > -1:
            lookup_dict = unsigned_integer_ranges
        else:
            lookup_dict = signed_integer_ranges
        
        #now we will check the minmax values against the lookup dictionary
        #we will use the sorted dictionary object
        for max_value_abs, dtype in lookup_dict.items():
            if amplitude <= max_value_abs:
                #we will select the smallest datatype that fits the range
                #
                #print(dtype)
                column_transforms.append(pl.col(column_name).cast(dtype))
                break
    if original == pl.DataFrame:
        return df.with_columns(column_transforms).collect()

    return df.with_columns(column_transforms)


def test_downcast_integers_eager():
    """We will generate an example dataframe with all the cases 
    of integers and ranges as shown in the example dataframe above
    We will also test with some not integer column types to make sure they are not changed
    We will not check the content but only the final datatype. for the lazy case, 
    we will collect before confiming the datatypes have been changed as expected
    We will specifiy the datatpe on read in case polars gets weird about how infer_schema works
    """
    #set up test data
    test_size = 100
    example_data = pl.DataFrame({
        "int8": np.random.randint(-127, 127, size=test_size, dtype=np.int64),
        "int16": np.random.randint(-32767, 32767, size=test_size, dtype=np.int64),
        "int32": np.random.randint(-2147483647, 2147483647, size=test_size, dtype=np.int64),
        "int64": np.random.randint(-9223372036854775807, 9223372036854775807, size=test_size, dtype=np.int64),
        "uint8": np.random.randint(0, 255, size=test_size, dtype=np.int64),
        "uint16": np.random.randint(0, 65535, size=test_size, dtype=np.int64),
        "uint32": np.random.randint(0, 4294967295, size=test_size, dtype=np.int64),
        "float32": np.random.rand(test_size),
        "float64": np.random.rand(test_size),
        "bool": np.random.randint(0, 1, size=test_size, dtype=np.int64),
        "str": np.random.choice(["a", "b", "c"], size=test_size),
    })
    #convert to csv and back again using stringio and polars
    example_data = pl.read_csv(StringIO(example_data.write_csv()))
    #print(f"Before conversion {example_data.estimated_size()/test_size} bytes/row  with {example_data.dtypes}")
    #convert integers
    start_type = type(example_data)
    downcasted_example_data = downcast_integers(example_data)
    #print(f"After Conversion {downcasted_example_data.estimated_size()/test_size} bytes/row with {downcasted_example_data.dtypes}")
    #check that the datatypes have been changed as expected
    
    assert start_type == type(downcasted_example_data), f"The type of the dataframe should be the same before and after conversion {start_type} != {type(downcasted_example_data)}"

    expected_dtypes = [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.Float64, pl.Float64, pl.UInt8, pl.Utf8]

    assert str(list(downcasted_example_data.dtypes)) == str(expected_dtypes),  f"Datatypes have not been changed as expected {downcasted_example_data.dtypes} != {expected_dtypes}"
    
    #check that the data has the same ranges (no cut offs)
    assert downcasted_example_data["int8"].min() >= -128, "int8 min value has been cut off"
    assert downcasted_example_data["int8"].max() <= 128, "int8 max value has been cut off"
    assert downcasted_example_data["int16"].min() >= -32768,   "int16 min value has been cut off"
    assert downcasted_example_data["int16"].max() <= 32768,  "int16 max value has been cut off"
    assert downcasted_example_data["int32"].min() >= -2147483648,   "int32 min value has been cut off"
    assert downcasted_example_data["int32"].max() <= 2147483648,    "int32 max value has been cut off"
    assert downcasted_example_data["int64"].min() >= -9223372036854775808, "int64 min value has been cut off"
    assert downcasted_example_data["int64"].max() <= 9223372036854775808,  "int64 max value has been cut off"
    assert downcasted_example_data["uint8"].min() >= 0, "uint8 min value has been cut off"
    assert downcasted_example_data["uint8"].max() <= 255, "uint8 max value has been cut off"
    assert downcasted_example_data["uint16"].min() >= 0, "uint16 min value has been cut off"
    assert downcasted_example_data["uint16"].max() <= 65535, "uint16 max value has been cut off"
    assert downcasted_example_data["uint32"].min() >= 0, "uint32 min value has been cut off"
    assert downcasted_example_data["uint32"].max() <= 4294967295, "uint32 max value has been cut off"


def test_downcast_integers_lazy():
    """We will generate an example dataframe with all the cases 
    of integers and ranges as shown in the example dataframe above
    We will also test with some not integer column types to make sure they are not changed
    We will not check the content but only the final datatype. for the lazy case, 
    we will collect before confiming the datatypes have been changed as expected
    We will specifiy the datatpe on read in case polars gets weird about how infer_schema works
    """
    #set up test data
    test_size = 100
    example_data = pl.DataFrame({
        "int8": np.random.randint(-127, 127, size=test_size, dtype=np.int64),
        "int16": np.random.randint(-32767, 32767, size=test_size, dtype=np.int64),
        "int32": np.random.randint(-2147483647, 2147483647, size=test_size, dtype=np.int64),
        "int64": np.random.randint(-9223372036854775807, 9223372036854775807, size=test_size, dtype=np.int64),
        "uint8": np.random.randint(0, 255, size=test_size, dtype=np.int64),
        "uint16": np.random.randint(0, 65535, size=test_size, dtype=np.int64),
        "uint32": np.random.randint(0, 4294967295, size=test_size, dtype=np.int64),
        "float32": np.random.rand(test_size),
        "float64": np.random.rand(test_size),
        "bool": np.random.randint(0, 1, size=test_size, dtype=np.int64),
        "str": np.random.choice(["a", "b", "c"], size=test_size),
    })
    #convert to csv and back again using stringio and polars
    example_data = pl.read_csv(StringIO(example_data.write_csv())).lazy()
    #print(f"Before conversion {example_data.estimated_size()/test_size} bytes/row  with {example_data.dtypes}")
    #convert integers
    start_type = type(example_data)
    downcasted_example_data = downcast_integers(example_data)
    #print(f"After Conversion {downcasted_example_data.estimated_size()/test_size} bytes/row with {downcasted_example_data.dtypes}")
    #check that the datatypes have been changed as expected
    
    assert start_type == type(downcasted_example_data), f"The type of the dataframe should be the same before and after conversion {start_type} != {type(downcasted_example_data)}"

    expected_dtypes = [pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.Float64, pl.Float64, pl.UInt8, pl.Utf8]

    assert str(list(downcasted_example_data.dtypes)) == str(expected_dtypes),  f"Datatypes have not been changed as expected {downcasted_example_data.dtypes} != {expected_dtypes}"
    
    #check that the data has the same ranges (no cut offs)
    if start_type == pl.LazyFrame:
        downcasted_example_data = downcasted_example_data.collect()
    assert downcasted_example_data["int8"].min() >= -128, "int8 min value has been cut off"
    assert downcasted_example_data["int8"].max() <= 128, "int8 max value has been cut off"
    assert downcasted_example_data["int16"].min() >= -32768,   "int16 min value has been cut off"
    assert downcasted_example_data["int16"].max() <= 32768,  "int16 max value has been cut off"
    assert downcasted_example_data["int32"].min() >= -2147483648,   "int32 min value has been cut off"
    assert downcasted_example_data["int32"].max() <= 2147483648,    "int32 max value has been cut off"
    assert downcasted_example_data["int64"].min() >= -9223372036854775808, "int64 min value has been cut off"
    assert downcasted_example_data["int64"].max() <= 9223372036854775808,  "int64 max value has been cut off"
    assert downcasted_example_data["uint8"].min() >= 0, "uint8 min value has been cut off"
    assert downcasted_example_data["uint8"].max() <= 255, "uint8 max value has been cut off"
    assert downcasted_example_data["uint16"].min() >= 0, "uint16 min value has been cut off"
    assert downcasted_example_data["uint16"].max() <= 65535, "uint16 max value has been cut off"
    assert downcasted_example_data["uint32"].min() >= 0, "uint32 min value has been cut off"
    assert downcasted_example_data["uint32"].max() <= 4294967295, "uint32 max value has been cut off"

test_downcast_integers_eager()
test_downcast_integers_lazy()
print(f"Tests passed")