-- Logische Schemadefinition der drei Tabellen in MinIO (Bucket mes-data).
-- Es gibt keinen Metastore, der diese DDL ausfuehrt: Das Schema liegt in den
-- Parquet-Dateien selbst. Diese Datei ist die lesbare Referenz dazu und
-- entspricht dem, was stream-processing/streaming_job.py schreibt.

-- Bronze Layer: normalisierte Rohereignisse aus dem Kafka-Topic machine-events
-- (foreachBatch, append, keine Watermark; jede Instanz schreibt ihren machine_type-Ordner)

CREATE TABLE IF NOT EXISTS bronze_machine_events (
    timestamp       TIMESTAMP,
    machine_id      STRING,
    machine_type    STRING,
    measurements    MAP<STRING, STRING>,   -- vollstaendige Messwert-Map aus dem Event
    temperature     DOUBLE,                -- aus measurements extrahiert, nullable
    pressure        DOUBLE,                -- nur Typ A
    vibration       DOUBLE,                -- nur Typ B
    status          STRING,                -- Typ A und C
    schema_version  STRING,
    event_date      DATE
)
USING PARQUET
PARTITIONED BY (machine_type, event_date)
LOCATION 's3a://mes-data/bronze/machine-events';

-- Silver Layer: vom Streaming-Job aggregierte und angereicherte Maschinenmetriken
-- pro 10-Sekunden-Fenster (foreachBatch, append, Watermark 30 s)

CREATE TABLE IF NOT EXISTS silver_machine_metrics (
    machine_id          STRING,
    machine_type        STRING,
    window_start        TIMESTAMP,
    window_end          TIMESTAMP,
    avg_temperature     DOUBLE,
    min_temperature     DOUBLE,
    max_temperature     DOUBLE,
    event_count         BIGINT,
    last_status         STRING,            -- letzter Status im Fenster, NULL bei Typ B
    temperature_limit   DOUBLE,            -- TEMP_LIMIT aus der ConfigMap zum Aggregationszeitpunkt
    limit_exceeded      BOOLEAN,           -- max_temperature > temperature_limit
    event_date          DATE
)
USING PARQUET
PARTITIONED BY (machine_type, event_date)
LOCATION 's3a://mes-data/silver/machine-metrics';

-- Silver Layer: zuletzt bekannter Status je Maschine
-- (foreachBatch im complete-Modus, overwrite je machine_type-Partition)

CREATE TABLE IF NOT EXISTS silver_machine_status (
    machine_id      STRING,
    machine_type    STRING,
    last_status     STRING,
    last_timestamp  TIMESTAMP
)
USING PARQUET
PARTITIONED BY (machine_type)
LOCATION 's3a://mes-data/silver/machine-status';
