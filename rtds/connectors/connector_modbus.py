import logging
import queue
from dataclasses import dataclass
from pyModbusTCP.client import ModbusClient
from models import TagType, Tag, TagValue

import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from connectors.connector_abc import ConnectorABC

log = logging.getLogger('ConnectorModbus')

@dataclass
class ConnectorModbus(ConnectorABC):
    '''
    ConnectorModbus v0.1
    connection_string: host=xx.xx.xx.xx; port=502; unit_id=1, timeout=xx; auto_open=true; auto_close=true
    '''
    host:str=None
    port:int=502
    unit_id:int=1
    timeout:float=30
    auto_open:bool=True
    auto_close:bool=True
    client:ModbusClient=None
    

    def __init__(self, name, cycle, connection_string, tags, read_queue, is_read_only=True, write_queue=None):
        super().__init__(name, cycle, connection_string, tags, read_queue, is_read_only, write_queue)
        try:
            self.host=self.connection_string['host']
            self.port=int(self.connection_string['port'])
            self.timeout=float(self.connection_string['timeout'])
            self.unit_id=int(self.connection_string['unit_id'])
            self.auto_open=self.connection_string['auto_open'].lower() in ['true']
            self.auto_close=self.connection_string['auto_close'].lower() in ['true']
        except Exception as e:
            log.error(f'''
connection_string must be:
'connection_string: host=xx.xx.xx.xx; port=502; unit_id=1; timeout=xx; auto_open=true; auto_close=true',
but got:
{connection_string}
''')
            raise e
        self.client = ModbusClient(host=self.host,
                                   port=self.port,
                                   unit_id=self.unit_id,
                                   auto_open=self.auto_open,
                                   auto_close=self.auto_close,
                                   timeout=self.timeout)
        log.debug(self)

    def _source_parse(self, source):
        #source = 'C:0:10' | 'DI:0:10' | 'RI:0:10' | 'RH:0:10'
        sl = source.upper().split(':')
        if len(sl) != 3:
            raise ValueError(f'source wrong format: {sl}')
        if sl[0] not in ['C','DI', 'RI', 'RH']:
            raise ValueError(f'source wrong format: {sl}, must be in list: C, DI, RI, RH')
        try:
            sl[1] = int(sl[1])
        except:
            raise ValueError(f'source wrong format: {sl}, addr {sl[1]} must be int')
        try:
            sl[2] = int(sl[2])
        except:
            raise ValueError(f'source wrong format: {sl}, count {sl[2]} must be int')
        return sl
        
    def _read_coils(self, addr, count):
        return self.client.read_coils(addr, count)

    def _read_discrete_inputs(self, addr, count):
        return self.client.read_discrete_inputs(addr, count)

    def _read_input_registers(self, addr, count):
        return self.client.read_input_registers(addr, count)

    def _read_holding_registers(self, addr, count):
        return self.client.read_holding_registers(addr, count)

    def _read(self, source):
        try:
            sl = self._source_parse(source)
        except ValueError as e:
            log.error(e.message)
            return None
        if sl[0] == 'C':
            return self._read_coils(sl[1], sl[2])
        elif sl[0] == 'DI':
            return self._read_discrete_inputs(sl[1], sl[2])
        elif sl[0] == 'RI':
            return self._read_input_registers(sl[1], sl[2])
        elif sl[0] == 'RH':
            return self._read_holding_registers(sl[1], sl[2])
        
    def open(self):
        if not self.auto_open:
            self.client.open()

    def close(self):
        if not self.auto_close:
            self.client.close()
        
    def read(self):
        log.debug(f'read cycle process start')
        for key, tag in self.tags:
            result_list = self._read(tag.source)
            if result_list is not None and len(result_list) == 1:
                value = result_list[0]
            else:
                value = result_list
            log.debug(f'read modbus address: {tag.source} and get value: {value}')
            tgv = TagValue(name=key, type_=tag.type_, status=0, value=value)
            self.read_queue.put(tgv)
        log.debug(f'read cycle processed')

    def write(self):
        log.debug(f'write cycle process start')
        if not self.is_read_only and self.write_queue is not None:
            while not self.write_queue.empty():
                value = self.write_queue.get()
                log.dbug(f'write tag: {value}')
        log.debug(f'write cycle processed')

if __name__ == '__main__':
    logging.basicConfig(level='DEBUG')
    log.info('test begin')    
    tags = {}
    read_queue = queue.Queue()
    write_queue = queue.Queue()
    
#    for i in range(1):
#        tag = Tag(name=f'tag_{i}', type_=TagType.STR, connector_name='modbus', source='RH:0:6')
#        tags[tag.name] = tag
        
    tag1 = Tag(name=f'tag_1', type_=TagType.INT, connector_name='modbus', source='RH:5:1')
    tags[tag1.name] = tag1

    tag2 = Tag(name=f'tag_2', type_=TagType.STR, connector_name='modbus', source='RI:0:6')
    tags[tag2.name] = tag2

    tag3 = Tag(name=f'tag_3', type_=TagType.STR, connector_name='modbus', source='C:0:4')
    tags[tag3.name] = tag3

    tag4 = Tag(name=f'tag_4', type_=TagType.STR, connector_name='modbus', source='DI:0:4')
    tags[tag4.name] = tag4

    for key, tag in tags.items():
        write_queue.put(TagValue(tag=tag))
        
    connector = ConnectorModbus(name='modbus',
                                cycle=1,
                                connection_string='host=localhost; port=502; unit_id=2; timeout=1; auto_open=false; auto_close=false',
                                tags=tags,
                                read_queue=read_queue,
                                write_queue=write_queue)
    connector.run()
    log.info('test end')    
