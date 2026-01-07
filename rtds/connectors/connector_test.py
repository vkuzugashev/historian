import math
from multiprocessing import Queue
import queue
import random as rnd
from typing import Dict
from connectors.connector_abc import ConnectorABC
from models.tag import TagType, Tag, TagValue
from loggers import logger

class ConnectorTest(ConnectorABC): 
    sources: Dict[str, dict] = {}

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
        super().__init__(log, name, cycle, connection_string, tags, read_queue, is_read_only, write_queue, description, metrics_queue)
        for _, tag in self.tags:
            if tag.source:
                # Парсим строку источника
                source_params = dict(map(str.strip, sub.split('=', 1)) for sub in tag.source.split(';') if '=' in sub)
                func = source_params.get('func').lower()
                period = float(source_params.get('period', 1))
                scale = float(source_params.get('scale', 1))
                if func not in ['sin', 'cos', 'rnd', 'line']:
                    raise Exception(f'Function "{func}" is not supported.')
                source = {
                    'func': func,
                    'period': period,
                    'scale': scale,
                    'phase': 0.0
                }
                self.sources[tag.name] = source
        self.log.debug(f'lodaed tags sourcecs {self.sources}')
        
    def calc_value(self, tag_name: str):
        source = self.sources.get(tag_name)
        if source:
            if source['func'] == 'line':
                return source['scale']
            elif source['func'] == 'rnd':
                return rnd.uniform(0, source['scale'])
            elif source['func'] == 'sin':
                result = source['scale'] * math.sin(math.radians(source['phase']))
            elif source['func'] == 'cos':
                result = source['scale'] * math.cos(math.radians(source['phase']))
            else:
                raise ValueError(f"Unsupported function: {source['func']}")

            # Инкрементируем фазу для периодических функций
            if source['func'] in ['sin', 'cos']:
                delta_phi = (360  * self.cycle) / (60.0 * source['period']) 
                source['phase'] += delta_phi
                if source['phase'] >= 360:
                    source['phase'] %= 360
                
                self.log.debug(f'{tag_name}.phase={self.sources[tag_name]['phase']}, {source['phase']}')

            return result
        else:
            return 0.0

    def read(self):
        self.log.debug(f'read cycle process start')
        for key, tag in self.tags:
            value = TagValue(name=key, type_=tag.type_, status=0, value=self.calc_value(key))
            self.read_queue.put(value)
            self.log.debug(f'read tag: {value}')
        self.log.debug(f'read cycle processed')

    def write(self):
        self.log.debug(f'write cycle process start')
        if not self.is_read_only and self.write_queue is not None:
            while not self.write_queue.empty():
                value = self.write_queue.get()
                log.debug(f'write tag: {value}')
        self.log.debug(f'write cycle processed')

if __name__ == '__main__':
    log = logger.get_logger('ConnectorTest')
    log.info('test begin')    
    tags = {}
    read_queue = queue.Queue()
    write_queue = queue.Queue()
    
    for i in range(1000):
        tag = Tag(name=f'tag_{i}', type_=TagType.INT, connector_name='connector_test', value=i)
        tags[tag.name] = tag
        
    for key in list(tags):
        tag = tags[key]
        write_queue.put(TagValue(tag=tag))
        
    connector = ConnectorTest(name='test', cycle=0.5, connection_string='connstr', tags=tags, read_queue=read_queue, write_queue=write_queue)
    connector.run()
    log.info('test end')    
