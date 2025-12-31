import os
from pathlib import Path
import sys
import time 
from prometheus_client import start_http_server, Counter, Summary
import multiprocessing as mp

sys.path.extend(['.','..'])

from loggers import logger
import configs.file as config
from models.tag import Tag, TagValue 
from models.command import CommandEnum, Command 
import storeges.sqldb as store
import api.server as api

tags = {}
connectors = {}
scripts = {}
processes = {}

log_queue = mp.Queue(-1)
store_queue = mp.Queue()
api_command_queue = mp.Queue() 

log = logger.get_default('server', log_queue)

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

def storage_run(log_queue, store_queue):
    log.info('storage process started')

    try:
        store.run(log_queue, store_queue)
    except Exception as e:
        log.error(f'storage process stoped, error: {e}')

def api_run(q):
    log.info('api process started')

    try:
        api.run(q)
    except Exception as e:
        log.error(f'api process stoped, error: {e}')

def api_command_handler():
    log.debug('api command reading...')
    
    if not api_command_queue.empty():
        command = api_command_queue.get()
        if isinstance(command, Command) and command.command_enum == CommandEnum.RELOAD:
            reload_config()
        else:
            log.warning(f'unknow command: {command.command_enum}')

    log.debug('api command reding stoped')

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

# загрузить конфигурацию
def load_config():
    global connectors, tags, scripts    
    connectors, tags, scripts = store.get_config(server=sys.modules[__name__])
    log.info(f'Loaded config, connectors: {len(connectors)}, tags: {len(tags)}, scripts: {len(scripts)}')
    return connectors, tags, scripts


def start_process(process_name, target, args):
    global processes
    p = mp.Process(target=target, args=args)
    p.start()
    processes[process_name] = p
    log.info(f'{process_name} started')

def stop_processes():
    for key, process in processes.items():
        process.terminate()
        process.join()
        log.info(f'process {key}, stoped')    

def start_connectors():
    global connectors
    for _, connector in connectors.items():
        p = mp.Process(target=connector_run, args=(connector,))
        p.start()
        processes[connector.name] = p
        log.info(f'connector {connector.name} started')

def reload_config():
    global connectors, tags, scripts
    
    log.info('configuration reloading started ...')
    
    for _, connector in connectors.items():
        p = processes[connector.name]
        p.terminate()
        p.join()
        log.info(f'process {connector.name}, stoped')
    
    connectors, tags, scripts = load_config()
    start_connectors()
    
    log.info('success reloaded configuration')

@REQUEST_LATENCY.time()
def request_cycle():
    for _, connector in sorted(connectors.items()):
        connector_read(connector)
    for _, script in sorted(scripts.items()):
        script.run()
    
def run():
    
    store.init_db()
    
    global connectors, tags
    connectors, tags, _ = load_config()

    start_process(process_name='storage', target=storage_run, args=(log_queue, store_queue,))
    start_process(process_name='api', target=api_run, args=(api_command_queue,))  

    start_connectors()

    time.sleep(5)
    log.info('server loop started')

    # Добавить cчетчики
    TAG_COUNTER.inc(len(tags))
    CONNECTOR_COUNTER.inc(len(connectors))

    try:
        while True:
            request_cycle()
            api_command_handler()
            time.sleep(0.1)
    except BaseException as e:
        log.error(f'server loop stoped, error: {e}')

    stop_processes()

if __name__ == '__main__':
    
    # Создаем QueueListener для обработки очереди
    logger.start()

    log.info(f'Example load configuration from file (ods): server.py config.ods')
    if len(sys.argv) > 1:
        log.info(f'load config from file: {sys.argv[1]}')
        configFile = os.path.join(str(Path(__name__).parent), sys.argv[1])
        connectors, tags, scripts = config.load_from_file(server=sys.modules[__name__], configFile=configFile)
        log.info(f'Connectors: {len(connectors)}, Tags: {len(tags)}, Scripts: {len(scripts)}')
        store.set_config(connectors, tags, scripts)

    log.info(store.DB_URL)
    # Start up the server to expose the metrics.    
    start_http_server(4000)
    run()
    logger.stop()
