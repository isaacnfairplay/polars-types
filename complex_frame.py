import polars as pl
from integers import downcast_integers
from strings import downcast_strings
from attributes import extract_attributes


class ComplexFrame:
    """
    This class defines the complex frame, which is a collection of
    all internal frames are primarily related or come from the same source frame 
    """
    
    
    def __init__(self, frame, attributes = None):
        self.frames = {}
        compressed_frame = downcast_integers(frame)
        categorized_frame = downcast_strings(compressed_frame)
        self.frames['attributes'], self.frame['facts'] = extract_attributes(categorized_frame)
    
    @classmethod
    def to_lazy(cls, frame):
        """
        Convert a complex frame to a lazy frame
        """
        #left join attributes to facts using cross join
        return frame.frames['facts'].join(
            frame.frames['attributes'], on = None, how = 'cross')
    
    @classmethod
    def concat(cls, frames: iter[ComplexFrame]):
        """
        Concatenate a list of frames into a single frame
        """
        lazy_frames = [cls.to_lazy(frame) for frame in frames]
        lazy_frame = pl.concat(lazy_frames)
        return cls(lazy_frame)
    
        
    
        
        
        
        
        