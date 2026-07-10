# producer/history_to_kafka.py
import os, sys
import time
from sqlalchemy import Engine, create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from kafka import KafkaProducer
import json
from dotenv import load_dotenv


sys.path.extend(['.','..'])

from loggers import logger
from store.sqldb import History, State, Tag
from metrics import server as metrics

load_dotenv()

log = None
shared_metrics_queue=None

# Настройки
DB_URL = os.getenv('STORE_DB_URL', "sqlite:///data/history.db")
STORE_SQL_ENGINE_ECHO = os.getenv('STORE_SQL_ENGINE_ECHO', 'false').lower() in ('true', '1', 'on','yes')
KAFKA_BOOTSTRAP_SERVERS = [host.strip() for host in os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')]
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC','history_data')
BATCH_SIZE = int(os.getenv('KAFKA_BATCH_SIZE', '100'))  # Количество записей в одном пакете

engine: Engine = None
producer: KafkaProducer = None
shared_metrics_queue = None

def get_engine():
    global engine
    if not engine:
        engine = create_engine(DB_URL, echo=STORE_SQL_ENGINE_ECHO)
    return engine

def init():
    global producer
    # Создаём engine и producer
    if not engine:
        log.info('create engine ...')
        _ = get_engine()
        log.info('engine created success')
    if not producer:        
        log.info('creating producer ...')
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
        log.info('producer created success')

def close_resources():
    if producer:
        log.info('Closing resources...')
        producer.flush()
        producer.close()

def send_history_batch(last_id: int) -> int:
    """
    Отправляет пачку записей из History в Kafka и обновляет State.
    Выбирает записи с ID > producer_last_id.
    """
    engine = get_engine()
    with Session(engine) as session:
        start_time = time.time()
        try:
            if last_id < 0:
            # Получаем последний отправленный ID из State
                state = session.execute(
                    select(State).where(State.id=='producer_last_id')
                    ).scalar_one_or_none()
                if not state:
                    log.info("State record for producer_last_id not found, set last_id=0!")
                    last_id = 0
                else:
                    log.info(f"State record for producer_last_id found: last_id={state.value}")
                    last_id = int(state.value)
            
            log.debug(f"Fetching history records after ID: {last_id}")

            # Выбираем следующие записи (упорядочено по tag_time, tag_id)
            stmt = (
                select(History, Tag)
                .join(Tag, History.tag_id == Tag.id)
                .where(History.id > last_id)
                .order_by(History.id)
                .limit(BATCH_SIZE)
            )
            rows = session.execute(stmt).all()

            if not rows:
                log.debug("No new history records to send.")
                time.sleep(0.1)
                return last_id

            # Подготовка данных для Kafka
            messages = []
            max_id = last_id

            for row in rows:
                msg = {
                    "tg": row.History.tag_id,
                    "tm": f'{row.History.tag_time.isoformat()}Z',
                    "tp": row.Tag.type_,
                    "st": row.History.status,
                    "bv": row.History.bool_value,
                    "iv": row.History.int_value,
                    "fv": row.History.float_value,
                    "vv": row.History.var_value
                }
                messages.append(msg)
                if max_id < row.History.id:
                    max_id = row.History.id  # Обновляем до последней временной метки

            # Отправка в Kafka
            log.debug(f'Sending message: count={len(messages)}')
            producer.send(KAFKA_TOPIC, value=messages).add_callback(success_callback).add_errback(error_callback)
            
            last_id = max_id
            log.debug(f"Sent {len(messages)} history records to Kafka. Last ID: {last_id}")
            
            # Обновляем State
            # Атомарный UPSERT для всех записей
            stmt = sqlite_insert(State).values({'id':'producer_last_id', 'value': f'{last_id}'})
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=[State.id], set_={"value": stmt.excluded.value, "description": "Last sended id to kafka"}
            )            
            session.execute(on_conflict_stmt)
            session.commit()
            log.debug(f"Updated state producer_last_id={last_id}")
            
            if shared_metrics_queue:
                shared_metrics_queue.put(
                    metrics.Metric(
                        name = metrics.MetricEnum.KAFKA_PRODUCER_DURATION, 
                        labels = ['ok'],
                        value  = time.time() - start_time
                    )
                )
            
            return last_id
        
        except KeyboardInterrupt:
            raise
        except Exception as e:
            session.rollback()
            log.error(f"Error sending history batch: {e}", exc_info=True)
            if shared_metrics_queue:
                shared_metrics_queue.put(
                    metrics.Metric(
                        name = metrics.MetricEnum.KAFKA_PRODUCER_DURATION, 
                        labels = ['error'],
                        value  = time.time() - start_time
                    )
                )
            return last_id

def success_callback(records):
    log.debug(f"Kafka delivery successful: partition={records.partition}, offset={records.offset}")

def error_callback(exception):
    log.error(f"Failed to deliver Kafka message: {exception}")

def run(log_queue=None, metrics_queue=None):    # Start up the server to expose the metrics.    
    global log, shared_metrics_queue

    if metrics_queue:
        shared_metrics_queue = metrics_queue

    log = logger.get_logger('producer', log_queue)
    log.info(f'Kafka producer started: KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}, KAFKA_TOPIC={KAFKA_TOPIC}')    
    
    init()

    last_collect_metrics = time.time()
    last_id = -1

    while True:        
        try:
            last_id = send_history_batch(last_id)
            if metrics_queue:
                if time.time() - last_collect_metrics > 60:
                    last_collect_metrics = time.time()
                    # Метрики kafka producer
                    metrics.collect_process_metrics('producer', metrics_queue)
        except KeyboardInterrupt:
            log.info('KeyboardInterrupt received. Exiting...')
            break
        except Exception as e:
            log.error(f'Unhandled exception: {e}', exc_info=True)

    close_resources()

if __name__ == "__main__":
    log = logger.get_logger('kafka_producer')
    send_history_batch()
