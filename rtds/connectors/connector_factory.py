from multiprocessing import Queue
from loggers import logger
from connectors.connector_info import ConnectorInfo
from connectors.connector_test import ConnectorTest
from connectors.connector_modbus import ConnectorModbus

def get_connector(
        log,
        name, 
        cycle, 
        connection_string:str, 
        tags, 
        read_queue:Queue=None, 
        is_read_only=True, 
        write_queue:Queue=None, 
        description=None, 
        metrics_queue:Queue=None):
    
    pars = connection_string.split(';')
    par_val = pars[0].split('=')
    if par_val[0].lower() != 'connector':
        raise Exception(f'Wrong connection_string format: {connection_string}, firs must be connector=ConnectorClass;..')
    
    if par_val[0].lower() == 'connector' and par_val[1].lower() == 'connectortest':
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

def run(connector_info:ConnectorInfo, log_queue:Queue, metrics_queue:Queue=None):    # Start up the server to expose the metrics.    
    log = logger.get_logger(connector_info.name, log_queue)
    try:
        connector = get_connector(
            log=log,
            name=connector_info.name, 
            cycle=connector_info.cycle, 
            connection_string=connector_info.connection_string,
            tags=connector_info.tags, 
            read_queue=connector_info.read_queue, 
            is_read_only=connector_info.is_read_only, 
            write_queue=connector_info.write_queue,
            metrics_queue=metrics_queue,
            description=connector_info.description
        )
        if connector:
            connector.run()
        else:
            log.error(f'Connector {connector_info.name} not found')
    except Exception as ex:
        log.error(f'Connector {connector_info.name} error: {ex}')
