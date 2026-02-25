import os, sys, time
from pathlib import Path
import multiprocessing as mp
from dotenv import load_dotenv

sys.path.extend(['.','..'])

from connectors import connector_factory
from connectors.connector_info import ConnectorInfo
from loggers import logger
from configs import config_ods
from models.tag import Tag, TagValue 
from models.command import CommandEnum, Command 
from store import sqldb as store
from api import server as api
from metrics import server as metrics
from producers import kafka_producer as producer

tags = {}
connector_infos = {}
scripts = {}
processes = {}

log_queue:mp.Queue = mp.Queue()
store_queue:mp.Queue = mp.Queue()
api_command_queue:mp.Queue = None
metrics_queue:mp.Queue = None

load_dotenv()

API_ENABLED = os.getenv('API_ENABLED', 'False').lower() == 'true'
METRICS_ENABLED = os.getenv('METRICS_ENABLED', 'False').lower() == 'true'
KAFKA_ENABLED = os.getenv('KAFKA_ENABLED', 'False').lower() == 'true'
PROCESS_STOP_TIMEOUT = float(os.getenv('PROCESS_STOP_TIMEOUT', '0.1'))

log = logger.get_logger('server', log_queue)

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
            store_queue.put(new_value)                            
    else:
        log.error(f'Unsupport type: {value}')

def set(value):
    if isinstance(value, TagValue):
        tag = tags[value.name]
        if tag is not None:
            if tag.connector_name is not None:
                connector = connector_infos[tag.connector_name]
                if connector.write_queue is not None:
                    connector.write_queue.put(value)
            else:
                _set(value)
    else:
        log.error(f'Unsupport type: {value}')

def storage_run(log_queue, store_queue, metrics_queue):
    log.info('storage process started')

    try:
        store.run(log_queue, store_queue, metrics_queue)
    except Exception as e:
        log.error(f'storage process stoped, error: {e}')

def api_run(log_queue, api_command_queue, metrics_queue):
    log.info('api process started')

    try:
        api.run(log_queue, api_command_queue, metrics_queue)
    except Exception as e:
        log.error(f'api process stoped, error: {e}')

def api_command_handler():
    if api_command_queue:
        log.debug('api command reading...')
    
        if not api_command_queue.empty():
            command = api_command_queue.get()
            if isinstance(command, Command):
                if command.command_enum == CommandEnum.RELOAD:
                    reload_config()
                elif command.command_enum == CommandEnum.CLEAR:
                    clear_config()
                else:
                    log.warning(f'unsupport command: {command.command_enum}') 
            else:
                log.warning(f'unknow type command: {command}')

        log.debug('api command reding stoped')

def metrics_run(log_queue, metrics_queue):
    log.info('metrics process started')
    try:
        metrics.run(4000, log_queue, metrics_queue)
    except Exception as e:
        log.error(f'metrics process stoped, error: {e}')

def producer_run(log_queue, metrics_queue):
    log.info('producer process started')
    try:
        producer.run(log_queue, metrics_queue)
    except Exception as e:
        log.error(f'producer process stoped, error: {e}')

def connector_run(connector_info:ConnectorInfo, log_queue, metrics_queue):
    try:
        connector_factory.run(connector_info, log_queue, metrics_queue)
    except Exception as e:
        log.error(f'connector {connector_info.name} stoped, error: {e}')

def connector_read(connector_info:ConnectorInfo):
    log.debug(f'connector {connector_info.name} read process start ...')

    while not connector_info.read_queue.empty():
        value = connector_info.read_queue.get()
        _set(value)

    log.debug(f'connector {connector_info.name} read process stop')

# загрузить конфигурацию
def load_config():
    global connector_infos, tags, scripts    
    connector_infos, tags, scripts = store.get_config(server=sys.modules[__name__])
    log.info(f'Loaded config, connectors: {len(connector_infos)}, tags: {len(tags)}, scripts: {len(scripts)}')
    return connector_infos, tags, scripts

def start_process(process_name, target, args):
    p = mp.Process(target=target, args=args)
    p.start()
    processes[process_name] = p
    log.info(f'{process_name} started')

def stop_processes():
    for key, process in processes.items():
        try:
            if not process.is_alive():
                log.warning(f'process {key} is stoped')
                continue
            process.terminate()
            process.join(PROCESS_STOP_TIMEOUT)
            log.info(f'process {key}, stoped')   
        except Exception as e:
            log.error(f'process {key}, stoped with error: {e}')

def check_processes():
    for key, process in processes.items():
        if not process.is_alive():
            raise Exception(f'Check process {key}, procees is stoped')            

def start_connectors(log_queue:mp.Queue, metrics_queue:mp.Queue):
    for connector_info in connector_infos.values():
        try:
            log.info(f'start connector {connector_info.name} ...')
            p = mp.Process(target=connector_run, args=(connector_info, log_queue, metrics_queue))
            p.start()
            processes[connector_info.name] = p
            log.info(f'connector {connector_info.name} started')
        except Exception as e:
            log.error(f'Fail start connector {connector_info.name}, error: {e}')

def stop_connectors():
    for connector_info in connector_infos.values():
        process = processes.get(connector_info.name)
        if process:
            try:
                if not process.is_alive():
                    log.warning(f'connector process {connector_info.name} is already stoped')
                    continue
                log.info(f'connector process {connector_info.name} stoping ...')
                process.terminate()
                process.join(PROCESS_STOP_TIMEOUT)
                log.info(f'connector process {connector_info.name}, stoped')
            except Exception as e:
                log.error(f'connector process {connector_info.name}, stoped with error: {e}')
            processes.pop(connector_info.name)
        else:
            log.warning(f'connector process {connector_info.name} not found')


def reload_config():   
    log.info('configuration reloading started ...')
    
    stop_connectors()
    _, _, _ = load_config()
    start_connectors()
    
    log.info('success reloaded configuration')

def clear_config():
    log.info('configuration clearing started ...')
    
    stop_connectors()
    store.clear_config()
    _, _, _ = load_config()
    start_connectors()
    
    log.info('success cleared configuration')

def scan_cycle():
    if METRICS_ENABLED:
        start_time = time.time()
    
    for connector_info in sorted(connector_infos.values()):
        connector_read(connector_info)
    for _, script in sorted(scripts.items()):
        script.run()
    
    if METRICS_ENABLED:
        duration = time.time() - start_time
        metrics_queue.put(metrics.Metric(metrics.MetricEnum.SCAN_CYCLE_LATENCY, duration))
    
    
def run():
    try: 
        store.init_db(log_queue)   
        _, _, _ = load_config()
        start_process(process_name='storage', target=storage_run, args=(log_queue, store_queue, metrics_queue, ))
        
        if API_ENABLED:
            start_process(process_name='api', target=api_run, args=(log_queue, api_command_queue, metrics_queue, ))  
        if METRICS_ENABLED:
            start_process(process_name='metrics', target=metrics_run, args=(log_queue, metrics_queue,))  
        if KAFKA_ENABLED:
            start_process(process_name='producer', target=producer_run, args=(log_queue, metrics_queue,))  

        start_connectors(log_queue, metrics_queue)
        log.info('wait 5 sec ...')
        time.sleep(5)
        log.info('server loop started')

        # Добавить cчетчики
        if METRICS_ENABLED:
            metrics_queue.put(metrics.Metric(metrics.MetricEnum.TAG_COUNTER, len(tags)))
            metrics_queue.put(metrics.Metric(metrics.MetricEnum.CONNECTOR_COUNTER, len(connector_infos)))

        last_collect_metrics = time.time()
        try:
            while True:
                check_processes()
                scan_cycle()
                api_command_handler()
                if time.time() - last_collect_metrics > 60:
                    metrics.collect_process_metrics('app', metrics_queue)
                time.sleep(0.1)
        except BaseException as e:
            log.error(f'server loop stoped, error: {e}')

    except BaseException as e:
        log.error(f'server stoped, error: {e}')
    finally:
        stop_connectors()
        stop_processes()

if __name__ == '__main__':
   
    if METRICS_ENABLED:
        metrics_queue = mp.Queue()

    if API_ENABLED:
        api_command_queue = mp.Queue()

    # Создаем QueueListener для обработки очереди
    logger.start()

    log.info(f'Example load configuration from file (ods): python3 app/server.py config.ods')
    if len(sys.argv) > 1:
        log.info(f'load config from file: {sys.argv[1]}')
        configFile = os.path.join(str(Path(__name__).parent), sys.argv[1])
        connector_infos, tags, scripts = config_ods.load_from_file(configFile=configFile)
        log.info(f'Connectors: {len(connector_infos)}, Tags: {len(tags)}, Scripts: {len(scripts)}')
        store.set_config(connector_infos, tags, scripts)
    try:
        run()
    except BaseException as e:
        log.error(f'server stoped, error: {e}')
    finally:
        logger.stop()
