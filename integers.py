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
    """Function downcasts integers to the smallest possible datatype. 
    It does this by checking the min and max values of each column and
    comparing it to the range of the datatypes. If the min and max values
    First it creates a list of compatable types and then selects the one with the lowest bit resolution.
    """
    orignal_type = type(df)
    df = df.lazy()
    schema_dict = df.schema
    cast_dict = OrderedDict
    for col in df.columns:
        if schema_dict[col] not in pl.INTEGER_DTYPES:
            continue
        #Get the min and max values of the column
        min_value = df.select(pl.col(col)).min().first().collect()[0]
        max_value = df.select(pl.col(col)).max().first().collect()[0]
        type_ranges = [type_range for type_range in type_ranges if type_range.min_value <= min_value and type_range.max_value >= max_value]
        if len(type_ranges) == 0:
            continue
        castype = sorted(type_ranges, key=lambda x: x.bit_resolution)[0] #sort by ascending bit resolution
        cast_dict[col] = castype.datatype
    df = df.with_columns([pl.col(col).cast(datatype) for col, datatype in cast_dict.items()])
    if type(df) != orignal_type:
        return df.collect()
    return df

def test_downcast_integers_eager():
    """Function to test downcast integers in eager mode"""
    
    