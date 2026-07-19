from multiprocessing import Queue
from dataclasses import dataclass

@dataclass
class ConnectorInfo():
    name:str = None
    cycle:int = None
    connection_string:dict = None
    tags:dict = None
    is_read_only:bool = None
    description:str = None
    read_queue:Queue = None
    write_queue:Queue = None

    def __init__(self, 
                 name:str, 
                 cycle:int, 
                 connection_string:str, 
                 tags:dict, 
                 is_read_only:bool, 
                 description:str):
        
        self.name = name
        self.cycle = cycle
        self.connection_string = connection_string
        self.tags = tags
        self.is_read_only = is_read_only
        self.description = description

        self.read_queue = Queue()
        if self.is_read_only==False:
            self.write_queue = Queue()