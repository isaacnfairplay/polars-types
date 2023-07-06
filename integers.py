import polars as pl
from collections import OrderedDict, namedtuple

#import module to display tracebacks
import traceback


IntegerRange = namedtuple("IntegerRange", ["datatype", "min_value", "max_value","bit_resolution"])

def calculate_int_range(datatype : pl.DataType)-> IntegerRange:
    """Function uses the name of the datatype to determine if it is signed or 
    unsigned and bit resolution and from that its range"""

    name = datatype.__name__
    bit_resolution = int(name.lower().split("int")[-1])                   
    unsigned = name[0].lower() == "u"
    if unsigned:
        min_value = 0
        max_value = 2**(bit_resolution)-1
    else:
        min_value = -2**(bit_resolution-1) 
        max_value = 2**(bit_resolution-1)-1
    return IntegerRange(datatype, min_value, max_value, bit_resolution)

type_ranges = {calculate_int_range(type) for type in pl.INTEGER_DTYPES}
print(type_ranges)




def downcast_integers(df):
    """Function takes a polars dataframe and downcasts
    the integer columns to the smallest 
    possible datatype"""
     

    start_frame_type = type(df)
    df = df.lazy()
    schema_dict = df.schema
    expressions = []
    
    for col in df.columns:
        if schema_dict[col] not in pl.INTEGER_DTYPES:
            continue
        #Get the min and max values of the column
        min_value = df.select(pl.col(col)).min().collect()[col].to_list()[0]
        max_value = df.select(pl.col(col)).max().collect()[col].to_list()[0]
        compatable_types = (type_range for type_range in type_ranges if type_range.min_value <= min_value and type_range.max_value >= max_value)
        sorted_types = sorted(compatable_types, key=lambda x: x.bit_resolution)
        if not sorted_types:
            continue
        selected_type = sorted_types[0]
        if selected_type.datatype == schema_dict[col]:
            continue
        expressions.append(pl.col(col).cast(selected_type.datatype))
    df = df.with_columns(expressions) if expressions else df
    df = df.collect() if start_frame_type == pl.DataFrame else df
    return df

def test_downcast_integers(datatype):
    import numpy as np

    if datatype not in pl.INTEGER_DTYPES:
        return True
    print(datatype)
    #calculate test range
    type_range = calculate_int_range(datatype)
    #generate test data

    test_data = list( range(type_range.min_value, type_range.max_value, (type_range.max_value-type_range.min_value)//1000))
    print(len(test_data))
    #create polars dataframe
    
    if min(test_data) ==0:
        df = pl.DataFrame({"test": test_data}).with_columns(pl.col("test").cast(pl.UInt64))
    else: 
        df = pl.DataFrame({"test": test_data}).with_columns(pl.col("test").cast(pl.Int64))
    
    print(df.dtypes)
    #test if downcasted correctly
    downcasted_df = downcast_integers(df)
    assert downcasted_df["test"].dtype == datatype, f"Downcasted to {downcasted_df['test'].dtype} but should be {datatype}"
    

def test_downcast_integers_all():
    for type in pl.INTEGER_DTYPES:
        test_downcast_integers(type)


def min_reproducable_example():
    import polars as pl
    import traceback
    pl.show_versions()
    min_value = 0 # min value for UInt64
    max_value = 2**64-1 # max value for UInt64
    print(f"Python polars version {pl.__version__}")
    values = list( range(min_value, max_value, (max_value-min_value)//10))
    print(f"the max test value is less than the max datatype storage value = {max_value > max(values)}")
    print({type(value) for value in values})
    try:
        df = pl.DataFrame({
            "test":  
                values
                })
        print(df.dtypes)
    except OverflowError as e:
        #print traceback for exception
        traceback.print_exc(limit=10)
        print(e)
    print(df.dtypes)
    


