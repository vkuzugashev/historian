import queue
import random as rnd
from connectors.connector_abc import ConnectorABC
from models.tag import TagType, Tag, TagValue
from loggers import logger

class ConnectorTest(ConnectorABC):

    def read(self):
        self.log.debug(f'read cycle process start')
        for key, tag in self.tags:
            value = TagValue(name=key, type_=tag.type_, status=0, value=rnd.uniform(0,50))
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
    log = logger.get_default('ConnectorTest')
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
