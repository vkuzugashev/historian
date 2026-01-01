from enum import Enum
from prometheus_client import start_http_server, Counter, Histogram
from loggers import logger

log = None
shared_metrics_queue = None

class MetricEnum(Enum):
    SCAN_CYCLE_LATENCY=0
    TAG_COUNTER=1
    CONNECTOR_COUNTER=2
    CONNECTOR_DURATION_CYCLE=3
    STORE_DURATION_CYCLE=4

class Metric:
    def __init__(self, name: MetricEnum, value, labels=None):
        self.name = name
        self.value = value
        self.labels = labels

def run(port=4000, log_queue=None, metrics_queue=None):    # Start up the server to expose the metrics.    
    global log, shared_metrics_queue

    log = logger.get_default('metrics', log_queue)

    if metrics_queue:
        shared_metrics_queue = metrics_queue

    start_http_server(port)

    if shared_metrics_queue:
        handle_metrics()
    else:
        log.error('metrics_queue is not defined')


# метрики общие
SCAN_CYCLE_LATENCY = Histogram('scan_cycle_latency_seconds', 'latency scan cycle')
TAG_COUNTER = Counter('tag_counter', 'Tags count')
CONNECTOR_COUNTER = Counter('connector_counter', 'Connectors count')

# метрики коннекторов
CONNECTOR_DURATION_CYCLE = Histogram('connector_duration_cycle_seconds', 'Duration connector cycle', ['connector'])

# метрики хранилища
STORE_DURATION_CYCLE = Histogram('store_duration_cycle_seconds', 'Duration store cycle')

def handle_metrics():
    if shared_metrics_queue:
        while True:
            if not shared_metrics_queue.empty():                
                metric = shared_metrics_queue.get()
                if isinstance(metric, Metric):
                    log.debug(f'handle_metrics: {metric}')
                    
                    match metric.name:
                        case MetricEnum.SCAN_CYCLE_LATENCY:
                            SCAN_CYCLE_LATENCY.observe(metric.value)
                        case MetricEnum.TAG_COUNTER:
                            TAG_COUNTER.inc(metric.value)
                        case MetricEnum.CONNECTOR_COUNTER:
                            CONNECTOR_COUNTER.inc(metric.value)
                        case MetricEnum.CONNECTOR_DURATION_CYCLE:
                            CONNECTOR_DURATION_CYCLE.labels(metric.labels).observe(metric.value)
                        case MetricEnum.STORE_DURATION_CYCLE:
                            STORE_DURATION_CYCLE.observe(metric.value)




