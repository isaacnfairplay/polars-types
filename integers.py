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

TYPE_RANGES = frozenset({calculate_int_range(type) for type in pl.INTEGER_DTYPES})





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
        compatable_types = (type_range for type_range in TYPE_RANGES if type_range.min_value <= min_value and type_range.max_value >= max_value)
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

def test_downcast_integers(datatype, mode= 'eager'):
    import numpy as np

    if datatype not in pl.INTEGER_DTYPES:
        return True

    #calculate test range
    type_range = calculate_int_range(datatype)
    #generate random 100 test points using Uint64 as part of the numpy function for unsigned datatypes
    test_data = np.random.randint(type_range.min_value, type_range.max_value, 100, 
               dtype=np.uint64 if type_range.min_value == 0 else np.int64)
    
    if min(test_data) ==0:
        df = pl.DataFrame({"test": test_data})
    else: 
        df = pl.DataFrame({"test": test_data})
        
    if mode=='lazy':
        df = df.lazy()
 
    #test if downcasted correctly
    downcasted_df = downcast_integers(df)
    downcasted_types = downcasted_df.schema
    
    assert type(downcasted_df) == type(df), f"Downcasted to {type(downcasted_df)} but should be {type(df)}"
    assert downcasted_types["test"] == datatype, f"Downcasted to {downcasted_types['test']} but should be {datatype}"
    

def test_downcast_integers_all():
    for type in pl.INTEGER_DTYPES:
        for mode in ['lazy', 'eager']:
            test_downcast_integers(type, mode=mode)
    print("All tests passed")



def _min_rep_example_polars_9748():
    print(f"min reproducable example for issue: https://github.com/pola-rs/polars/issues/9748")
    import polars as pl
    import traceback
    import numpy as np
    pl.show_versions()
    min_value = 0 # min value for UInt64
    max_value = 2**64-1 # max value for UInt64
    print(f"Python polars version {pl.__version__}")
    values = np.random.randint(min_value, max_value, 100, dtype=np.uint64 )
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



