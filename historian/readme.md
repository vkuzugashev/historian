# Historian — Система сбора и хранения исторических данных

`historian` — это микросервис для приёма, десериализации и хранения временных рядов (истории тегов) из Kafka в TimescaleDB. Проект предназначен для работы в распределённой системе сбора данных, где источники публикуют изменения значений тегов, а `historian` сохраняет их в оптимизированной для временных рядов базе данных.

---

## 📦 Назначение

- Приём данных через **Kafka**.
- Десериализация JSON-сообщений, содержащих массив значений тегов.
- Хранение данных в **PostgreSQL + TimescaleDB** с разделением по типам:
  - `bool` → `history_bools`
  - `int` → `history_integers`
  - `float` → `history_floats`
  - `str`, `datetime`, `array` → `history_strings`
- Поддержка гипертаблиц и политики хранения (retention).
- Мониторинг через **Prometheus** (экспорт метрик времени обработки).

---

## 🧩 Архитектура
```
[Источники]
↓ (JSON-массивы)
Kafka (topic: history_data)
↓
[ historian — Kafka Consumer ]
↓ (bulk insert)
TimescaleDB (historydb)
↓
[ Prometheus ← /metrics ]
```
---

## 🔧 Технологии

| Компонент       | Используется |
|----------------|-------------|
| Язык           | Python 3.9+ |
| Брокер         | Kafka       |
| База данных    | PostgreSQL + [TimescaleDB](https://www.timescale.com/) |
| ORM            | SQLAlchemy  |
| Метрики        | Prometheus Client |
| Логирование    | Python logging + QueueHandler |
| Переменные окружения | `python-dotenv` |

---

## ⚙️ Настройка

### Установка зависимостей

```bash
pip install -r requirements.txt
```

### База данных

Убедитесь, что:

- Запущен PostgreSQL с расширением TimescaleDB.
- Создана база данных historydb.

Инициализация TimescaleDB
Выполните SQL-скрипт store/init.sql:
```bash
psql -U postgres -h localhost -f store/init.sql
```

⚠️ Убедитесь, что TimescaleDB установлен:

```SQL
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
```


### 🛠 Конфигурация (.env)

```env
# Уровень логирования
LOG_LEVEL=INFO

# Параметры хранилища
STORE_BATCH_SIZE = 100
STORE_HISTORY_HOURS = 24
STORE_SQL_ENGINE_ECHO = False
STORE_DB_URL = postgresql://postgres:1@postgres:5432/historydb

# API
API_PORT = 5002

# Kafka
KAFKA_BOOTSTRAP_SERVERS = kafka:9092,kafka:9094
KAFKA_TOPIC = history_data
KAFKA_GROUP_ID = history_consumer
KAFKA_AUTO_COMMIT_INTERVAL_MS = 5000
KAFKA_SESSION_TIMEOUT_MS = 10000
KAFKA_BATCH_SIZE = 3
```

## ▶️ Запуск

```bash
python app.py
```

После запуска:

- Сервис подключится к Kafka.
- Начнёт читать сообщения из топика history_data.
- Сохранять данные в TimescaleDB.
- Экспортировать метрики на /metrics (в будущем можно добавить FastAPI/Flask для HTTP-сервера).


## 📈 Метрики (Prometheus)

Доступны следующие метрики:
- consumer_duration_seconds_bucket — время выполнения операций консьюмера.
- store_duration_seconds_bucket — время записи в БД.

Пример:
```
# HELP consumer_duration consumer methods duration
# TYPE consumer_duration histogram
consumer_duration_seconds_bucket{method="deserialize_message",status="ok",le="0.0001"} 1.0
```

## 📥 Формат сообщений в Kafka
Ожидается JSON-массив объектов:
```json
[
  {
    "tg": "MOTOR_1.RUN",
    "tm": "2025-04-05T10:00:00Z",
    "tp": "bool",
    "st": 0,
    "bv": true
  },
  {
    "tg": "SENSOR_1.TEMP",
    "tm": "2025-04-05T10:00:01Z",
    "tp": "float",
    "st": 0,
    "fv": 45.6
  }
]
```

| Поле | Описание |
|------|----------|
| tg | tag_id — идентификатор тега |
| tm | tag_time — временная метка (ISO 8601) |
| tp | tag_type — тип: bool, int, float, str, datetime, array |
| st | status — статус качества сигнала |
| bv | bool_value — значение для типа bool |
| iv | int_value — значение для типа int |
| fv | float_value — значение для типа float |
| vv | var_value — строковое значение для остальных типов |

## 📁 Структура проекта
```
historian/
├── app.py                  # Основной консьюмер
├── .env                    # Переменные окружения
├── requirements.txt        # Зависимости
├── readme.md               # Этот файл
├── loggers/
│   └── logger.py           # Гибкая система логирования с QueueHandler
├── metrics/
│   └── server.py           # Экспорт метрик Prometheus
├── models/
│   └── history_message.py  # Dataclass для сообщения
├── store/
│   ├── sqldb.py            # Работа с БД, ORM модели, bulk-запись
│   └── init.sql            # Инициализация TimescaleDb
```

## 🧪 Тестирование
- Запустите Kafka и PostgreSQL/TimescaleDB (можно через Docker).
- Выполните init.sql.
- Запустите app.py.
- Отправьте тестовое сообщение в Kafka:
```Bash
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic history_data
> [{"tg":"TEST.TAG","tm":"2025-04-05T12:00:00Z","tp":"int","st":0,"iv":123}]
```
- Проверьте, что данные появились в таблице history_integers.

## 🚀 Развитие (Roadmap)
 Добавить HTTP API (FastAPI) для чтения истории.
 Поддержка сжатия сообщений в Kafka (gzip, snappy).
 Асинхронная запись в БД (asyncpg).
 Автоматическая миграция с помощью Alembic.
 Health-check endpoint (/health).

## 📎 Лицензия
MIT (уточните при необходимости)