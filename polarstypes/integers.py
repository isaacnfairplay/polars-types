from collections import namedtuple
from typing import TypeVar
import polars as pl
DataFrameType = TypeVar('DataFrameType', pl.LazyFrame, pl.DataFrame)

#import module to display tracebacks

IntegerRange = namedtuple("IntegerRange", ["datatype", "min_value", "max_value","bit_resolution"])

def calculate_int_range(datatype : pl.DataType )-> IntegerRange:
    """Function uses the name of the datatype to determine if it is signed or 
    unsigned and bit resolution and from that its range"""

    name = str(datatype)
    bit_resolution = int(name.lower().rsplit("int", maxsplit=1)[-1])                   
    unsigned = name[0].lower() == "u"
    if unsigned:
        min_value = 0
        max_value = 2**(bit_resolution)-1
    else:
        min_value = -2**(bit_resolution-1) 
        max_value = 2**(bit_resolution-1)-1
    return IntegerRange(datatype, min_value, max_value, bit_resolution)

TYPE_RANGES = frozenset({calculate_int_range(type) for type in pl.INTEGER_DTYPES})

def downcast_integers(frame: DataFrameType) -> DataFrameType:
    """ Function takes a polars frame and downcasts
    the integer columns to the smallest 
    possible datatype"""
     

    start_frame_type = type(frame)
    frame = frame.lazy()
    expressions = []
    
    for col, dtype in frame.schema.items():
        if dtype not in pl.INTEGER_DTYPES:
            continue
        #Get the min and max values of the column
        #LazyFrame->shortLazyFrame->shortDataFrame - > value at 0,0
        min_value = frame.select(col).min().collect().item()
        max_value = frame.select(col).max().collect().item()
        if max_value is None:
            continue
        if min_value is None:
            min_value = 0
        compatable_types = (type_range for type_range in TYPE_RANGES if type_range.min_value <= min_value and type_range.max_value >= max_value)
        sorted_types = sorted(compatable_types, key=lambda x: x.bit_resolution)
        if not sorted_types:
            continue
        selected_type = sorted_types[0]
        if selected_type.datatype == dtype:
            continue
        expressions.append(pl.col(col).cast(selected_type.datatype))
    frame = frame.with_columns(expressions) if expressions else frame
    frame = frame.collect() if start_frame_type == pl.DataFrame else frame
    return frame

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
        frame = pl.DataFrame({"test": list(test_data)})
    else:
        frame = pl.DataFrame({"test": list(test_data)})
    null_frame = pl.DataFrame({'test': [None]}).with_columns(pl.col('test').cast(int))
    frame = pl.concat([frame, null_frame], how='vertical_relaxed')
    if mode=='lazy':
        frame = frame.lazy()
 
    #test if downcasted correctly
    downcasted_frame = downcast_integers(frame)
    downcasted_types = downcasted_frame.schema
    
    assert isinstance(downcasted_frame,type(frame)), f"Downcasted to {type(downcasted_frame)} but should be {type(frame)}"
    assert downcasted_types["test"] == datatype, f"Downcasted to {downcasted_types['test']} but should be {datatype}"
    

def test_downcast_integers_all():
    for dtype in pl.INTEGER_DTYPES:
        if 'UInt64' in str(dtype):
            print("Skipping UInt64")
            continue
        for mode in ['lazy', 'eager']:
            test_downcast_integers(dtype, mode=mode)
    print("All tests passed")

#test_downcast_integers_all()