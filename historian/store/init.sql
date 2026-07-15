-- Подключение к базе данных, которую хотим расширить
\c historydb

-- Активируем расширение TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Создаем таблицы БЕЗ PRIMARY KEY для лучшей производительности TimescaleDB
-- (Составной PRIMARY KEY создает лишний индекс и мешает эффективному удалению чанков)
CREATE TABLE history_bools (
    tag_time TIMESTAMP WITH TIME ZONE NOT NULL,
    tag_id VARCHAR(50) NOT NULL,
    status INTEGER NOT NULL,
    value BOOLEAN
);

CREATE TABLE history_integers (
    tag_time TIMESTAMP WITH TIME ZONE NOT NULL,
    tag_id VARCHAR(50) NOT NULL,
    status INTEGER NOT NULL,
    value INTEGER
);

CREATE TABLE history_floats (
    tag_time TIMESTAMP WITH TIME ZONE NOT NULL,
    tag_id VARCHAR(50) NOT NULL,
    status INTEGER NOT NULL,
    value FLOAT
);

CREATE TABLE history_strings (
    tag_time TIMESTAMP WITH TIME ZONE NOT NULL,
    tag_id VARCHAR(50) NOT NULL,
    tag_type VARCHAR(10) NOT NULL,
    status INTEGER NOT NULL,
    value VARCHAR(500)
);

-- Преобразуем в гипертаблицы с размером чанка 1 час
-- (Это критически важно для быстрого удаления данных старше 4 часов)
SELECT create_hypertable('history_bools', 'tag_time', chunk_time_interval => INTERVAL '1 hour');
SELECT create_hypertable('history_floats', 'tag_time', chunk_time_interval => INTERVAL '1 hour');
SELECT create_hypertable('history_integers', 'tag_time', chunk_time_interval => INTERVAL '1 hour');
SELECT create_hypertable('history_strings', 'tag_time', chunk_time_interval => INTERVAL '1 hour');

-- Создаем индексы для быстрых запросов
CREATE INDEX idx_bools_time ON history_bools (tag_time DESC);
CREATE INDEX idx_bools_tag_id ON history_bools (tag_id);

CREATE INDEX idx_integers_time ON history_integers (tag_time DESC);
CREATE INDEX idx_integers_tag_id ON history_integers (tag_id);

CREATE INDEX idx_floats_time ON history_floats (tag_time DESC);
CREATE INDEX idx_floats_tag_id ON history_floats (tag_id);

CREATE INDEX idx_strings_time ON history_strings (tag_time DESC);
CREATE INDEX idx_strings_tag_id ON history_strings (tag_id);

-- Устанавливаем политику хранения данных
-- Удаляем данные старше 4 часов, проверяем каждые 15 минут
SELECT add_retention_policy('history_bools', drop_after => INTERVAL '4 hours', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE);
SELECT add_retention_policy('history_floats', drop_after => INTERVAL '4 hours', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE);
SELECT add_retention_policy('history_integers', drop_after => INTERVAL '4 hours', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE);
SELECT add_retention_policy('history_strings', drop_after => INTERVAL '4 hours', schedule_interval => INTERVAL '15 minutes', if_not_exists => TRUE);

-- Для оптимизации работы с 100 млн записей и ограничением памяти 350 МБ
-- Ограничиваем количество фоновых процессов
ALTER SYSTEM SET timescaledb.max_cron_jobs = 8;
ALTER SYSTEM SET timescaledb.bg_workers_max = 8;
SELECT pg_reload_conf();

-- Убеждаемся, что фоновый планировщик включен
SELECT timescaledb_background_scheduler_enabled('on');