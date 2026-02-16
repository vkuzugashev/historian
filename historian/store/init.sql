-- Подключение к базе данных, которую хотим расширить
\c historydb

-- Активируем расширение TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE history_bools (
        tag_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        tag_id VARCHAR(50) NOT NULL,
        status INTEGER NOT NULL,
        value BOOLEAN,
        PRIMARY KEY (tag_time, tag_id)
);

CREATE TABLE history_integers (
        tag_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        tag_id VARCHAR(50) NOT NULL,
        status INTEGER NOT NULL,
        value INTEGER,
        PRIMARY KEY (tag_time, tag_id)
);


CREATE TABLE history_floats (
        tag_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        tag_id VARCHAR(50) NOT NULL,
        status INTEGER NOT NULL,
        value FLOAT,
        PRIMARY KEY (tag_time, tag_id)
);


CREATE TABLE history_strings (
        tag_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
        tag_id VARCHAR(50) NOT NULL,
        tag_type VARCHAR(10) NOT NULL,
        status INTEGER NOT NULL,
        value VARCHAR(500),
        PRIMARY KEY (tag_time, tag_id)
);

SELECT create_hypertable('history_bools', 'tag_time');
SELECT create_hypertable('history_floats', 'tag_time');
SELECT create_hypertable('history_integers', 'tag_time');
SELECT create_hypertable('history_strings', 'tag_time');

