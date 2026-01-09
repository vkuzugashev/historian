# producer/history_to_kafka.py
import os, sys
import time
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from kafka import KafkaProducer
import json
from dotenv import load_dotenv


sys.path.extend(['.','..'])

from loggers import logger
from store.sqldb import History, State
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

engine = None
producer = None
shared_metrics_queue = None

def init():
    global engine, producer
    # Создаём engine и producer
    if not engine:
        log.info('create engine ...')
        engine = create_engine(DB_URL, echo=STORE_SQL_ENGINE_ECHO)
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
        producer.close()

def send_history_batch():
    """
    Отправляет пачку записей из History в Kafka и обновляет State.
    Выбирает записи с ID > producer_last_id.
    """
    with Session(engine) as session:
        try:
            start_time = time.time()

            # Получаем последний отправленный ID из State
            state = session.execute(
                select(State).where(State.id=='producer_last_id')).scalar_one_or_none()
            if not state:
                log.error("State record not found!")
                last_id = 0
            else:
                last_id = int(state.value)
            
            log.debug(f"Fetching history records after ID: {last_id}")

            # Выбираем следующие записи (упорядочено по tag_time, tag_id)
            stmt = (
                select(History)
                .where(History.id > last_id)
                .order_by(History.id)
                .limit(BATCH_SIZE)
            )
            rows = session.scalars(stmt).all()

            if not rows:
                log.debug("No new history records to send.")
                return

            # Подготовка данных для Kafka
            messages = []
            max_id = last_id

            for row in rows:
                msg = {
                    "tg": row.tag_id,
                    "tm": f'{row.tag_time.isoformat()}Z',
                    "st": row.status,
                    "bv": row.bool_value,
                    "iv": row.int_value,
                    "fv": row.float_value,
                    "sv": row.str_value
                }
                messages.append(msg)
                max_id = row.id  # Обновляем до последней временной метки

            # Отправка в Kafka
            log.debug(f'Sending message: count={len(messages)}')
            producer.send(KAFKA_TOPIC, value=messages).add_callback(success_callback).add_errback(error_callback)
            producer.flush()  # Ждём подтверждения отправки

            # Обновляем State
            # Атомарный UPSERT для всех записей
            stmt = sqlite_insert(State).values({'id':'producer_last_id', 'value': f'{max_id}'})
            on_conflict_stmt = stmt.on_conflict_do_update(
                index_elements=[State.id], set_={"value": stmt.excluded.value}
            )            
            session.execute(on_conflict_stmt)
            session.commit()

            log.debug(f"Sent {len(messages)} history records to Kafka. Last ID: {max_id}")
            if shared_metrics_queue:
                shared_metrics_queue.put(
                    metrics.Metric(
                        name = metrics.MetricEnum.KAFKA_PRODUCER_DURATION, 
                        labels = ['ok'],
                        value  = time.time() - start_time
                    )
                )
        
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
            raise

def success_callback(records):
    log.debug(f"Kafka delivery successful: partition={records.partition}, offset={records.offset}")

def error_callback(exception):
    log.debug(f"Failed to deliver Kafka message: {exception}")

def run(log_queue=None, metrics_queue=None):    # Start up the server to expose the metrics.    
    global log, shared_metrics_queue

    if metrics_queue:
        shared_metrics_queue = metrics_queue

    log = logger.get_logger('producer', log_queue)
    log.info(f'Kafka producer started: KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}, KAFKA_TOPIC={KAFKA_TOPIC}')    
    
    while True:        
        try:
            init()
            send_history_batch()
            time.sleep(0.1)  # Задержка между отправками
        except KeyboardInterrupt:
            log.info('KeyboardInterrupt received. Exiting...')
            break
        except Exception as e:
            log.error(f'Unhandled exception: {e}', exc_info=True)

    close_resources()

if __name__ == "__main__":
    log = logger.get_logger('kafka_producer')
    send_history_batch()
