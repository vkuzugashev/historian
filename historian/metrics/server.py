from prometheus_client import start_http_server, Counter, Histogram

# метрики консумера
CONSUMER_DURATION = Histogram(
    name='consumer_duration',
    documentation='consumer methods duration',
    labelnames=['method', 'status'],
    unit='sec',
    buckets=(0.0001, 0.0005, 0.01, 0.5, 1.05, 2.05, 3.05, 4.05, 5.05, 10.05, 15.05, 30.05, 60.05, 90.05, 120.05)
)

# метрики хранилища
STORE_DURATION = Histogram(
    name='store_duration',
    documentation='store methods duration',
    labelnames=['method','status'],
    unit='sec',
    buckets=(0.005, 0.01, 0.05, 0.1, 0.5, 1)
)

