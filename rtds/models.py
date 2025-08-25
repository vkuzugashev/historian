import json
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

class TagType(Enum):
    BOOL=0
    INT=1
    FLOAT=2
    STR=3

def get_tag_type(type_:str):
    if type_.lower() == 'bool':
        return TagType.BOOL
    if type_.lower() == 'int':
        return TagType.INT
    elif type_.lower() == 'float':
        return TagType.FLOAT
    elif type_.lower() == 'str':
        return TagType.STR
    else:
        raise Exception('Unsupport tag type')

@dataclass
class Tag:
    name: str = 'noname'
    type_: TagType = None
    source: str = None
    min_: float = None
    max_: float = None
    status: int = 0
    update_time: datetime = None
    value: object = None
    is_log: bool = False
    connector_name = None

    def __init__(self, name, type_, source=None, min_=None, max_=None, connector_name=None, is_log=False, value=0):
        self.name = name
        self.type_ = type_
        self.source = source
        self.min_ = min_
        self.max_ = max_
        self.connector_name = connector_name
        self.is_log = is_log
        if type_ == TagType.BOOL:
            self.value = bool(value)
        elif type_ == TagType.INT:
            self.value = int(value)
        elif type_ == TagType.FLOAT:
            self.value = float(value)
        else:                
            self.value = value

    def set(self, value, status):
        self.status = status
        self.update_time = datetime.utcnow()
        if self.min_ == self.max_:
            self.value = value
        elif value < self.min_:
            self.value = self.min_
            self.status = -1
        elif value > self.max_:
            self.value = self.max_
            self.status = -1
        else:
            self.value = value
        return TagValue(self)

    def toJSON(self):
        return json.dumps(
            self,
            default=lambda o: o.__dict__, 
            sort_keys=True,
            indent=4)
    
@dataclass
class TagValue:
    name: str = None
    type_: TagType = None
    status: int = 0
    update_time: datetime = None
    value: object = None

    def __init__(self, tag=None, name=None, type_=None, status=None, value=None):
        if isinstance(tag, Tag):
            self.name = tag.name
            self.type_ = tag.type_
            self.status = tag.status
            self.update_time = tag.update_time
            if tag.type_ == TagType.BOOL:
                self.value = bool(tag.value)
            elif tag.type_ == TagType.INT:
                self.value = int(tag.value)
            elif tag.type_ == TagType.FLOAT:
                self.value = float(tag.value)
            else:                
                self.value = tag.value
        else:
            self.name = name
            self.type_ = type_
            self.status = status
            self.update_time = datetime.utcnow()
            self.value = value
            
