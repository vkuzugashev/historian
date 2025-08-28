import logging 
import time 
import sys 
import multiprocessing as mp 
import json 
import config 
from models import Tag, TagValue 
import storage

log = logging.getLogger('server')

tags = {}
connectors = {}
scripts = {}
processes = {}
store_queue = mp.Queue()    

def add(tag):
    if isinstance(tag, Tag):
        tags[tag.name] = tag
    else:
        log.error(f'Unsupport type: {tag}')

def get(name):
    tag = tags[name]
    if tag is not None:
        return TagValue(tag)
    else:
        return None

def _set(value):
    if isinstance(value, TagValue):
        tag = tags[value.name]
        if tag is not None:
             new_value = tag.set(value.value, value.status)
             if tag.is_log:
                 store_queue.put(new_value)                            
    else:
        log.error(f'Unsupport type: {value}')

def set(value):
    if isinstance(value, TagValue):
        tag = tags[value.name]
        if tag is not None:
            if tag.connector_name is not None:
                connector = connectors[tag.connector_name]
                if connector.write_queue is not None:
                    connector.write_queue.put(value)
            else:
                _set(value)
    else:
        log.error(f'Unsupport type: {value}')

def storage_run(q):
    log.info('storage process started')
    try:
        storage.run(q)
    except Exception as e:
        log.error(f'storage process stoped, error: {e}')
        
def connector_run(connector):
    try:
        connector.run()
    except Exception as e:
        log.error(f'connector {connector.name} stoped, error: {e}')

def connector_read(connector):
    log.debug(f'connector {connector.name} read process start ...')
    while not connector.read_queue.empty():
        value = connector.read_queue.get()
        _set(value)
    log.debug(f'connector {connector.name} read process stop')
    
def run():
    global connectors, tags, scripts
    connectors, tags, scripts = config.load(server=sys.modules[__name__])
    p = mp.Process(target=storage_run, args=(store_queue,))
    p.start()
    processes['storage'] = p
    log.info(f'storage started')

    for _, connector in connectors.items():
        p = mp.Process(target=connector_run, args=(connector,))
        p.start()
        processes[connector.name] = p
        log.info(f'connector {connector.name} started')

    time.sleep(5)
    log.info('server loop started')

    try:
        while True:
            for _, connector in sorted(connectors.items()):
                connector_read(connector)
            for _, script in sorted(scripts.items()):
                script.run()
            time.sleep(1)
    except BaseException as e:
        log.error(f'server loop stoped, error: {e}')

    for key, process in processes.items():
        process.terminate()
        process.join()
        log.info(f'process {key}, stoped')

if __name__ == '__main__':    
    logging.basicConfig(level='INFO')
    run()
