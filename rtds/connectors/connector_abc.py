from abc import ABC
from multiprocessing import Queue
import time
from dataclasses import dataclass
import metrics.server as metrics

@dataclass
class ConnectorABC(ABC):
    log = None
    name:str = None
    cycle:int = None
    connection_string:dict = None
    tags = None
    read_queue:Queue = None
    write_queue:Queue = None
    is_read_only:bool = None
    description:str = None
    metrics_queue:Queue = None

    def __init__(self, log, name, cycle, connection_string, tags, read_queue, is_read_only=True, write_queue=None, description=None, metrics_queue=None):
        self.log = log
        self.name = name
        self.cycle = cycle
        self.tags = tags
        self.read_queue = read_queue
        self.is_read_only = is_read_only
        self.description = description
        if not is_read_only:
            self.write_queue = write_queue
        self.connection_string = dict(map(str.strip, sub.split('=', 1)) for sub in connection_string.split(';') if '=' in sub)
        self.metrics_queue = metrics_queue

    def open(self):
        pass

    def close(self):
        pass

    def read(self):
        pass

    def write(self):
        pass

    def __pause(self):
        self.log.debug(f'pause: {self.cycle} sec')
        time.sleep(self.cycle)    

    def run(self):
        while True:
            start_time = time.time()
            try:
                self.open()
                self.read()
                self.__pause()
                self.write()
            finally:
                self.close()
                duration = time.time() - start_time
                if self.metrics_queue:
                    self.metrics_queue.put(
                        metrics.Metric(
                            name   = metrics.MetricEnum.CONNECTOR_DURATION_CYCLE, 
                            labels = [self.name],
                            value  = duration
                        )
                    )
                else:
                    self.log.warning('no metrics_queue')

