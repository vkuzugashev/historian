from enum import Enum
import os
import time
from typing import Iterable
from prometheus_client import start_http_server, Counter, Histogram, Gauge
import psutil
from loggers import logger

log = None
shared_metrics_queue = None

class MetricEnum(Enum):
    SCAN_CYCLE_LATENCY=0
    TAG_COUNTER=1
    CONNECTOR_COUNTER=2
    CONNECTOR_DURATION=3
    STORE_DURATION=4
    STORE_SIZE_GAUGE=5
    STORE_ROWS_GAUGE=6
    SCRIPT_DURATION=7
    KAFKA_PRODUCER_DURATION=8
    PROCESS_CPU_USAGE=9
    PROCESS_MEMORY_USAGE=10    


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
    name='script_duration',
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

# Метрики локального файла
STORE_SIZE_GAUGE = Gauge(
    name='store_size_gauge',
    documentation='store size',
    unit='mb'
)
STORE_ROWS_GAUGE = Gauge(
    name='store_rows_gauge',
    documentation='store row count',
    unit='count'
)

PROCESS_CPU_USAGE = Gauge(
    name ='process_cpu_percent',
    documentation='Загрузка CPU процессом, %',
    labelnames=['process']
)
PROCESS_MEMORY_USAGE = Gauge(
    name='process_memory_mb', 
    documentation='Использование памяти процессом, МБ',
    labelnames=['process']
)

def handle_metrics():
    if shared_metrics_queue:
        last_time = time.time()
        while True:
            try:                    
                if shared_metrics_queue.empty():
                    if time.time() - last_time > 60:
                        collect_process_metrics('metrics', shared_metrics_queue)
                        last_time = time.time()
                    time.sleep(0.1)
                    continue

                metric = shared_metrics_queue.get()
                if not isinstance(metric, Metric):
                    log.warning(f'Unsupport type: {metric}')
                    continue

                log.debug(f'handle_metrics: {metric}')
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
                    case MetricEnum.STORE_SIZE_GAUGE:
                        STORE_SIZE_GAUGE.set(metric.value)
                    case MetricEnum.STORE_ROWS_GAUGE:
                        STORE_ROWS_GAUGE.set(metric.value)
                    case MetricEnum.SCRIPT_DURATION:
                        SCRIPT_DURATION.labels(*metric.labels).observe(metric.value)
                    case MetricEnum.KAFKA_PRODUCER_DURATION:
                        KAFKA_PRODUCER_DURATION.labels(*metric.labels).observe(metric.value)
                    case MetricEnum.PROCESS_CPU_USAGE:
                        PROCESS_CPU_USAGE.labels(*metric.labels).set(metric.value)
                    case MetricEnum.PROCESS_MEMORY_USAGE:
                        PROCESS_MEMORY_USAGE.labels(*metric.labels).set(metric.value)
                    case _:
                        log.warning(f'Unsupported metric: {metric}')
            except KeyboardInterrupt:
                log.warning(f'KeyboardInterrupt received. Exiting ...')
                break
            except Exception as e:
                log.error(f'fail set metric: {metric.name}, {e}')

def collect_process_metrics(process_name, metrics_queue):
    if metrics_queue:
        try: 
            process = psutil.Process(os.getpid())

            # CPU usage (%)
            cpu_percent = process.cpu_percent(interval=None)  # non-blocking
            metrics_queue.put(
                Metric(
                    name = MetricEnum.PROCESS_CPU_USAGE,
                    labels= [process_name],
                    value = cpu_percent
                )
            )
            
            # Memory usage (MB)
            memory_mb = process.memory_info().rss / 1024 / 1024  # RSS in MB
            metrics_queue.put(
                Metric(
                    name = MetricEnum.PROCESS_MEMORY_USAGE,
                    labels= [process_name],
                    value = memory_mb
                )
            )

        except Exception as e:
            log.warning(f'fail get process metrics: {e}')


