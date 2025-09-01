from abc import ABC
import time
import logging
from dataclasses import dataclass

log = logging.getLogger('ConnectorABC')

@dataclass
class ConnectorABC(ABC):
    name:str = None
    cycle:int = None
    connection_string:dict = None
    tags = None
    read_queue = None
    write_queue = None
    is_read_only:bool = None
    description:str = None

    def __init__(self, name, cycle, connection_string, tags, read_queue, is_read_only=True, write_queue=None, description=None):
        self.name = name
        self.cycle = cycle
        self.tags = tags
        self.read_queue = read_queue
        self.is_read_only = is_read_only
        self.description = description
        if not is_read_only:
            self.write_queue = write_queue
        self.connection_string = dict(map(str.strip, sub.split('=', 1)) for sub in connection_string.split(';') if '=' in sub)

    def open(self):
        pass

    def close(self):
        pass

    def read(self):
        pass

    def write(self):
        pass

    def __pause(self):
        log.debug(f'pause: {self.cycle} sec')
        time.sleep(self.cycle)    

    def run(self):
        while True:
            try:
                self.open()
                self.read()
                self.__pause()
                self.write()
            finally:
                self.close()

