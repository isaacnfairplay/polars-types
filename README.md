# polars-types
This library is centered around down casting datatypes in python polars. hopefully this kind of feature will be standard eventually
The library can help polars users achieve the memory compression offered by the underlying 
data format used for python-polars - a columnar data store

There are so far two key functions

One function that identifies integer ranges and selects the appropriate integer datatype (bit resolution and signedness)
One function that converts non-unique string columns to categorical columns

Future:
- itentify non-repeating values as attributes or as type literal
