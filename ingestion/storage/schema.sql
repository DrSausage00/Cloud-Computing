-- Bronze Layer: Raw data ingestion and storage
-- Persistiert die normaliiserten Machinenereignisse

CREATE TABLE IF NOT EXISTS bronze_machine_events (
    timestamp TIMESTAMP,
    machine_id      STRING,
    machine_type    STRING,
    temperature     DOUBLE,
    pressure        DOUBLE,
    vibration       DOUBLE,
    status          STRING, 
    event_date      DATE
)

USING DELTA
PARTINIONED BY (event_date)
LOCATION 's3a://mes-data/bronze/machine-events';

-- Silver Layer
-- Enthält die vom Streaming-Job aggregierten und angereicherten Maschiemetriken pro Zeitfenster

CREATE TABLE IF NOT EXISTS silver_machine_metrics ( 
    machine_id          STRING,
    machine_type        STRING,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    avg_temperature     DOUBLE,
    min_temperature     DOUBLE,
    max_temperature     DOUBLE,

    event_count         BIGINT,
    last_status         STRING,
    temperature_limit   DOUBLE,
    limit_exceeded      BOOLEAN,

    event_date          DATE
)

USING DELTA
PARTITIONED BY (event_date)
LOCATION 's3a://mes-data/silver/machine-metrics';