# consumer/kafka_consumer.py

import os, sys
import json
from typing import List
import logging
from kafka import KafkaConsumer

sys.path.extend(['.','..'])

# Подключение к вашим модулям
from historian.store import sqldb as store
from historian.models.history_message import HistoryMessage
from dotenv import load_dotenv

load_dotenv()

# === Настройки из .env ===
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092').split(',')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'history_data')
KAFKA_GROUP_ID = os.getenv('KAFKA_GROUP_ID', 'history_consumer')
KAFKA_AUTO_COMMIT_INTERVAL_MS = int(os.getenv('KAFKA_AUTO_COMMIT_INTERVAL_MS', '5000'))
KAFKA_SESSION_TIMEOUT_MS = int(os.getenv('KAFKA_SESSION_TIMEOUT_MS', '10000'))


def deserialize_message(value: bytes) -> List[HistoryMessage]:
    """
    Десериализует JSON-сообщение в список объектов HistoryMessage.
    
    Ожидается, что сообщение — это JSON-массив:
    [
        {"tg": "...", "tm": "...", ...},
        ...
    ]
    """    
    try:
        data = json.loads(value.decode('utf-8'))
        if isinstance(data, str):            
            data = json.loads(data.strip('"'))
        if not isinstance(data, list):
            log.error(f"Expected list of messages, got {type(data)}")
            return []

        messages = []
        for item in data:
            msg = HistoryMessage(
                tag_id=item.get('tg'),
                tag_time=item.get('tm'),
                tag_type=item.get('tp'),
                status=item.get('st'),
                bool_value=item.get('bv'),
                int_value=item.get('iv'),
                float_value=item.get('fv'),
                array_value=item.get('av')
            )
            messages.append(msg)
        return messages
    except Exception as e:
        log.error(f"Failed to deserialize message: {e}", exc_info=True)
        return []


def start_consumer():
    """Запускает Kafka Consumer для чтения и сохранения данных."""

    log.info(f"Kafka consumer starting. KAFKA_BOOTSTRAP_SERVERS={KAFKA_BOOTSTRAP_SERVERS}, KAFKA_TOPIC={KAFKA_TOPIC} ...")

    # Инициализируем БД
    store.init_db()

    # Создаём consumer
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset='earliest',  # начать с начала, если нет offset earliest | latest
        enable_auto_commit=True,
        auto_commit_interval_ms=KAFKA_AUTO_COMMIT_INTERVAL_MS,
        session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS,
        value_deserializer=lambda v: v,  # десериализуем вручную
        consumer_timeout_ms=-1,  # таймаут для проверки остановки
        heartbeat_interval_ms=3000,
    )

    log.info(f"Kafka consumer started. Listening to topic '{KAFKA_TOPIC}'...")

    try:
        for message in consumer:
            if message.value is None:
                log.debug("Received empty message (tombstone)")
                continue

            log.debug(f"Received message with {len(message.value)} bytes")

            # Десериализуем
            items = deserialize_message(message.value)
            if not items:
                log.warning("No valid items in message")
                continue

            # Сохраняем пачку
            try:
                store.store(items)
                log.debug(f"Stored {len(items)} history messages")
            except Exception as e:
                log.error(f"Failed to store batch: {e}")
                # Kafka не зафиксирует offset → сообщение будет перечитано
                continue

    except KeyboardInterrupt:
        log.info("Consumer stopped by user")
    except Exception as e:
        log.critical(f"Consumer crashed: {e}", exc_info=True)
    finally:
        consumer.close()
        log.info("Kafka consumer stopped")


# === Запуск ===
if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s:%(levelname)-8s:%(name)s:%(funcName)s():%(lineno)d: %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    log = logging.getLogger('kafka_consumer')
    start_consumer()
