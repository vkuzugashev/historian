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
    start_cycle_time:float = None

    def __init__(self, 
                 log, 
                 name:str, 
                 cycle:int, 
                 connection_string:str, 
                 tags, 
                 read_queue:Queue=None, 
                 is_read_only:bool=True, 
                 write_queue:Queue=None, 
                 description:str=None, 
                 metrics_queue:Queue=None):
        
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

        self.log.info(f'loaded {len(self.tags)} tags')


    def open(self):
        pass

    def close(self):
        pass

    def read(self):
        pass

    def write(self):
        pass

    def __pause(self):
        pause = self.cycle - (time.time() - self.start_cycle_time)
        self.log.debug(f'pause: {pause} sec')
        if pause > 0:
            time.sleep(pause)    

    def run(self):
        while True:
            self.start_cycle_time = time.time()
            try:
                start_time = time.time()
                self.open()
                if self.metrics_queue:
                    self.metrics_queue.put(
                        metrics.Metric(
                            name   = metrics.MetricEnum.CONNECTOR_DURATION, 
                            labels = [self.name, 'open', 'ok'],
                            value  = time.time() - start_time
                        )
                    )
                
                start_time = time.time()
                self.read()
                if self.metrics_queue:
                    self.metrics_queue.put(
                        metrics.Metric(
                            name   = metrics.MetricEnum.CONNECTOR_DURATION, 
                            labels = [self.name, 'read', 'ok'],
                            value  = time.time() - start_time
                        )
                    )
                
                start_time = time.time()
                self.write()
                if self.metrics_queue:
                    self.metrics_queue.put(
                        metrics.Metric(
                            name   = metrics.MetricEnum.CONNECTOR_DURATION, 
                            labels = [self.name, 'write', 'ok'],
                            value  = time.time() - start_time
                        )
                    )
                
                self.__pause()

                if self.metrics_queue:
                    self.metrics_queue.put(
                        metrics.Metric(
                            name   = metrics.MetricEnum.CONNECTOR_DURATION, 
                            labels = [self.name, 'cycle', 'ok'],
                            value  = time.time() - self.start_cycle_time
                        )
                    )
            
            except KeyboardInterrupt:
                self.log.warning(f'KeyboardInterrupt received. Exiting {self.name}...')
                break

            except Exception as e:
                if self.metrics_queue:
                    self.metrics_queue.put(
                        metrics.Metric(
                            name   = metrics.MetricEnum.CONNECTOR_DURATION, 
                            labels = [self.name, 'cycle', 'error'],
                            value  = time.time() - self.start_cycle_time
                        )
                    )
            finally:
                self.close()

