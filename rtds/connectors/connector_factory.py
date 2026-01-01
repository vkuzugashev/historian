from multiprocessing import Queue
from loggers import logger
from connectors.connector_test import ConnectorTest
from connectors.connector_modbus import ConnectorModbus

def get_connector(
        name, 
        cycle, 
        connection_string:str, 
        tags, 
        read_queue, 
        is_read_only=True, 
        write_queue=None, 
        log_queue=None, 
        description=None, 
        metrics_queue:Queue=None):
    
    pars = connection_string.split(';')
    par_val = pars[0].split('=')
    if par_val[0].lower() != 'connector':
        raise Exception(f'Wrong connection_string format: {connection_string}, firs must be connector=ConnectorClass;..')
    
    if par_val[0].lower() == 'connector' and par_val[1].lower() == 'connectortest':
        log = logger.get_default(name, log_queue)
        return ConnectorTest(log = log,
                             name=name,                              
                             cycle=cycle,
                             connection_string=connection_string, 
                             tags=tags, 
                             read_queue=read_queue, 
                             is_read_only=is_read_only, 
                             write_queue=write_queue,
                             metrics_queue=metrics_queue,
                             description=description)
    elif par_val[0].lower() == 'connector' and par_val[1].lower() == 'modbus':
        log = logger.get_default(name, log_queue)
        return ConnectorModbus(log=log,
                               name=name, 
                               cycle=cycle, 
                               connection_string=connection_string, 
                               tags=tags, 
                               read_queue=read_queue, 
                               is_read_only=is_read_only, 
                               write_queue=write_queue,
                               metrics_queue=metrics_queue,
                               description=description)
    else:
        raise Exception(f'Unsupport connector: {par_val[1]}, connection_string: {connection_string}')