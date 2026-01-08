from enum import Enum
import time
from typing import Iterable
from prometheus_client import start_http_server, Counter, Histogram
from loggers import logger

log = None
shared_metrics_queue = None

class MetricEnum(Enum):
    SCAN_CYCLE_LATENCY=0
    TAG_COUNTER=1
    CONNECTOR_COUNTER=2
    CONNECTOR_DURATION=3
    STORE_DURATION=4
    SCRIPT_DURATION=5
    KAFKA_PRODUCER_DURATION=6

class Metric:
    def __init__(self, name: MetricEnum, value: float, labels: Iterable[str]=None):
        self.name = name
        self.value = value
        self.labels = labels

def run(port=4000, log_queue=None, metrics_queue=None):    # Start up the server to expose the metrics.    
    global log, shared_metrics_queue

    log = logger.get_logger('metrics', log_queue)

    if metrics_queue:
        shared_metrics_queue = metrics_queue

    start_http_server(port)

    if shared_metrics_queue:
        handle_metrics()
    else:
        log.error('metrics_queue is not defined')


# метрики общие
SCAN_CYCLE_LATENCY = Histogram(
    name='scan_cycle_latency',
    documentation='latency scan cycle',
    unit='sec',
    buckets=(0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05)
)
TAG_COUNTER = Counter('tag_counter', 'Tags count')
CONNECTOR_COUNTER = Counter('connector_counter', 'Connectors count')

# метрики коннекторов
CONNECTOR_DURATION = Histogram(
    name='connector_duration',
    documentation='connector methods duration',
    labelnames=['connector','method', 'status'],
    unit='sec',
    buckets=(0.0001, 0.0005, 0.01, 0.5, 1.05, 2.05, 3.05, 4.05, 5.05, 10.05, 15.05, 30.05, 60.05, 90.05, 120.05)
)
# метрики скриптов
SCRIPT_DURATION = Histogram(
    name='scrypt_duration',
    documentation='script execute duration',
    labelnames=['script', 'status'],
    unit='sec',
    buckets=(0.00001, 0.00005, 0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1)
)
# метрики хранилища
STORE_DURATION = Histogram(
    name='store_duration',
    documentation='store methods duration',
    labelnames=['method','status'],
    unit='sec',
    buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1)
)
# метрики kafka producer
KAFKA_PRODUCER_DURATION = Histogram(
    name='kafka_producer_duration',
    documentation='kafka producer methods duration',
    labelnames=['status'],
    unit='sec',
    buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1)
)


def handle_metrics():
    if shared_metrics_queue:
        while True:
            if not shared_metrics_queue.empty():                
                metric = shared_metrics_queue.get()
                if isinstance(metric, Metric):
                    log.debug(f'handle_metrics: {metric}')
                    try:                    
                        match metric.name:
                            case MetricEnum.SCAN_CYCLE_LATENCY:
                                SCAN_CYCLE_LATENCY.observe(metric.value)
                            case MetricEnum.TAG_COUNTER:
                                TAG_COUNTER.inc(metric.value)
                            case MetricEnum.CONNECTOR_COUNTER:
                                CONNECTOR_COUNTER.inc(metric.value)
                            case MetricEnum.CONNECTOR_DURATION:
                                CONNECTOR_DURATION.labels(*metric.labels).observe(metric.value)
                            case MetricEnum.STORE_DURATION:
                                STORE_DURATION.labels(*metric.labels).observe(metric.value)
                            case MetricEnum.SCRIPT_DURATION:
                                SCRIPT_DURATION.labels(*metric.labels).observe(metric.value)
                            case MetricEnum.KAFKA_PRODUCER_DURATION:
                                KAFKA_PRODUCER_DURATION.labels(*metric.labels).observe(metric.value)
                    except Exception as e:
                        log.error(f'fail set metric: {metric.name}, {e}')
            else:
                time.sleep(0.1)




