import logging 
from os import path
import os
from pathlib import Path
import sys
import time 
import multiprocessing as mp 
import config 
from models import Tag, TagValue 
import storage
from prometheus_client import start_http_server, Counter, Summary

log = logging.getLogger('server')

tags = {}
connectors = {}
scripts = {}
processes = {}
store_queue = mp.Queue()    

# метрики
REQUEST_LATENCY = Summary('request_latency_seconds', 'Description of histogram')
TAG_COUNTER = Counter('tag_counter', 'Tags count')
CONNECTOR_COUNTER = Counter('connector_counter', 'Connectors count')

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

@REQUEST_LATENCY.time()
def request_cycle():
    for _, connector in sorted(connectors.items()):
        connector_read(connector)
    for _, script in sorted(scripts.items()):
        script.run()
    
def run():
    global connectors, tags, scripts
    
    connectors, tags, scripts = storage.get_config(server=sys.modules[__name__])
    log.info(f'Connectors: {len(connectors)}, Tags: {len(tags)}, Scripts: {len(scripts)}')
    log.info(scripts)    
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

    # Добавить cчетчики
    TAG_COUNTER.inc(len(tags))
    CONNECTOR_COUNTER.inc(len(connectors))

    try:
        while True:
            request_cycle()
            time.sleep(1)
    except BaseException as e:
        log.error(f'server loop stoped, error: {e}')

    for key, process in processes.items():
        process.terminate()
        process.join()
        log.info(f'process {key}, stoped')

if __name__ == '__main__':    
    logging.basicConfig(level='INFO')
    log.info(f'Example load configuration from file (ods): server.py config.ods')
    if len(sys.argv) > 1:
        log.info(f'load config from file: {sys.argv[1]}')
        configFile = os.path.join(str(Path(__name__).parent), sys.argv[1])
        connectors, tags, scripts = config.load_from_file(server=sys.modules[__name__], configFile=configFile)
        log.info(f'Connectors: {len(connectors)}, Tags: {len(tags)}, Scripts: {len(scripts)}')
        storage.set_config(connectors, tags, scripts)

    # Start up the server to expose the metrics.    
    start_http_server(4000)
    run()
