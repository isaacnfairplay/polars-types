import polars as pl
from collections import OrderedDict, namedtuple
#Testing
from io import StringIO
import numpy as np


IntegerRange = namedtuple("IntegerRange", ["datatype", "min_value", "max_value","bit_resolution"])

def calculate_int_range(datatype : pl.DataType)-> IntegerRange:
    """Function uses the name of the datatype to determine if it is signed or 
    unsigned and bit resolution and from that its range"""

    name = datatype.__name__
    bit_resolution = int(name.lower().split("int")[-1])                   
    signed = name[0].lower() == "u"
    if signed:
        min_value = -2**(bit_resolution-1) #we subtract 1 because we need to account for 0
        max_value = 2**(bit_resolution-1)-1 
    else:
        min_value = 0
        max_value = 2**(bit_resolution)-1
    return IntegerRange(datatype, min_value, max_value, bit_resolution)

type_ranges = {calculate_int_range(type) for type in pl.INTEGER_DTYPES}



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
        min_value = df.select(pl.col(col)).min().first().collect()[0]
        max_value = df.select(pl.col(col)).max().first().collect()[0]
        type_ranges = (type_range for type_range in type_ranges if type_range.min_value <= min_value and type_range.max_value >= max_value)
        selected_type = sorted(type_ranges, key=lambda x: x.bit_resolution)[0]
        if not type_ranges:
            continue
        expressions.append(selected_type.col(col).cast(selected_type.datatype))
    df = df.with_columns(expressions) if expressions else df
    df = df.collect() if start_frame_type == pl.DataFrame else df
    return df


    