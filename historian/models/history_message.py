from dataclasses import dataclass
import time

@dataclass
class HistoryMessage:
    tag_id:str=None
    tag_time:time=None
    status:int=None
    bool_value:bool=None
    int_value:int=None
    float_value:float=None
    str_value:str=None