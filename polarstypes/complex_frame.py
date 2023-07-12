import polars as pl
from integers import downcast_integers
from strings import downcast_strings
from attributes import extract_attributes
from relaxed_concat import majority_schema_concat
from typing import Optional
import os

class ComplexFrame:
    """
    This class defines the complex frame, which is a collection of
    all internal frames are primarily related or come from the same source frame 
    """
    io_dict = {'input':
        {'csv': pl.read_csv, 'parquet': pl.read_parquet, 'ipc': pl.read_ipc, 'json': pl.read_json},
        'output':
        {'csv': pl.DataFrame.to_csv, 'parquet': pl.DataFrame.to_parquet, 'ipc': pl.DataFrame.to_ipc, 'json': pl.DataFrame.to_json}
    }
    
    def __init__(self, frame : pl.LazyFrame | pl.LazyFrame, attributes: Optional[pl.DataFrame | dict] = None):
        self.frames = {}
        compressed_frame = downcast_integers(frame)
        categorized_frame = downcast_strings(compressed_frame)
        self.frames['attributes'], self.frames['facts'] = extract_attributes(categorized_frame)
        if attributes is not None:
            self.frames['attributes'] = attributes
    
    def to_lazy(self, frame):
        """
        Convert a complex frame to a lazy frame
        """
        #left join attributes to facts using cross join
        return self.frames['facts'].join(
            self.frames['attributes'], on = None, how = 'cross')
    @staticmethod
    def write_directory(self, directory_path: str, how : str = 'parquet' | 'csv' | 'json' | 'ipc' ):
        """
        Write a complex frame to a directory
        """
        directory_info = []
        for name, frame in self.frames.items():
            directory_info.append(f"{directory_path}\\{name}.{how}")
            self.io_dict['output'][how](frame, directory_info[-1])
        return directory_info
    
    @classmethod
    def read_directory(cls, directory_path: str):
        """
        Read a complex frame from a directory
        """
        frames = {}
        for filepath in os.listdir(directory_path):
            name = filepath.split('.')[-1]
            how = filepath.split('.')[-2]
            frames[name] = cls.io_dict['input'][how](f"{directory_path}\\{filepath}")
        return cls(frames['attributes'], frames['facts'])
            
        


def complex_concat(frames: iter[ComplexFrame]):
    """
    Concatenate a list of frames into a single frame
    """
    lazy_frames = [frame.to_lazy(frame) for frame in frames]
    lazy_frame = majority_schema_concat(lazy_frames)
    return ComplexFrame(lazy_frame)

    
        
    
        
        
        
        
        