import polars as pl
import numpy as np
import sys
from collections import OrderedDict
from io import StringIO
import time
#Create example data in dataframe with various integer ranges
# for all the different integer datatypes in python polars
#The data types will represent int8, int16, int32, int64, uint8, uint16, uint32, uint64 
#all data will be randomly generated using numpy random
#The data will be stored in a dataframe with 8 columns and 1000 rows
#The data will originally have int64 data types



#This dictionary will define the ranges of each singed integer datatype
#The key will be the max /min of the  datatype
signed_integer_ranges = OrderedDict(
    [
        (128, pl.Int8),
        (32768, pl.Int16),
        (2147483648, pl.Int32,),
        (9223372036854775808, pl.Int64),
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
    try:
        df = df.lazy()
        original = pl.DataFrame
    except AttributeError:
        original = pl.LazyFrame
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
        for max_value, dtype in lookup_dict.items():
            if amplitude < max_value:
                #we will select the smallest datatype that fits the range
                #
                #print(dtype)
                column_transforms.append(pl.col(column_name).cast(dtype))
                break
    if original == pl.DataFrame:
        df.with_columns(column_transforms).collect()

    return df.with_columns(column_transforms)

         
#Lets test the function on the example data
#We will print out the original data types and the new data types

# set print to string io so print statements can be captured

text_data = ["test_size,time,mode\n"]

for mode in ["lazy", "eager"]:
    for i in range(100):
        for size in range(2,64):
            test_size = 2*size
            #print(f"Test size is {test_size}")
            example_data = pl.DataFrame({
            "int8": np.random.randint(-128, 127, size=test_size, dtype=np.int64),
            "int16": np.random.randint(-32768, 32767, size=test_size, dtype=np.int64),
            "int32": np.random.randint(-2147483648, 2147483647, size=test_size, dtype=np.int64),
            "int64": np.random.randint(-9223372036854775808, 9223372036854775807, size=test_size, dtype=np.int64),
            "uint8": np.random.randint(0, 255, size=test_size, dtype=np.int64),
            "uint16": np.random.randint(0, 65535, size=test_size, dtype=np.int64),
            "uint32": np.random.randint(0, 4294967295, size=test_size, dtype=np.int64),
            
            
            },infer_schema_length=test_size)
            #convert to csv and back again using stringio and polars

            example_data = pl.read_csv(StringIO(example_data.write_csv()))
            if mode == "lazy":
                example_data = example_data.lazy()

            #print(f"Before conversion {example_data.estimated_size()/test_size} bytes/row  with {example_data.dtypes}")

            start = time.time()
            downcasted_example_data = downcast_integers(example_data).collect()

            text_data.append(f"{test_size},{(time.time() - start)},{mode}\n")
            #print(f"After Conversion {downcasted_example_data.estimated_size()/test_size} bytes/row with {downcasted_example_data.dtypes}")

with open(r"C:\Users\imoor\repositories\polars-types\integer_compress\downcast_integers.csv", "w") as f:
    f.writelines(text_data)










