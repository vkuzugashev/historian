import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent))

from connectors.connector_test import ConnectorTest
from connectors.connector_modbus import ConnectorModbus

def get_connector(name, cycle, connection_string:str, tags, read_queue, is_read_only=True, write_queue=None):
    pars = connection_string.split(';')
    par_val = pars[0].split('=')
    
    if par_val[0].lower() != 'connector':
        raise Exception(f'Wrong connection_string format: {connection_string}, firs must be connector=ConnectorClass;..')
    
    if par_val[0].lower() == 'connector' and par_val[1].lower() == 'connectortest':
        return ConnectorTest(name=name, 
                             cycle=cycle, 
                            connection_string=connection_string, 
                            tags=tags, 
                            read_queue=read_queue, 
                            is_read_only=is_read_only, 
                            write_queue=write_queue)
    elif par_val[0].lower() == 'connector' and par_val[1].lower() == 'modbus':
        return ConnectorModbus(name=name, 
                             cycle=cycle, 
                            connection_string=connection_string, 
                            tags=tags, 
                            read_queue=read_queue, 
                            is_read_only=is_read_only, 
                            write_queue=write_queue)
    else:
        raise Exception(f'Unsupport connector: {par_val[1]}, connection_string: {connection_string}')