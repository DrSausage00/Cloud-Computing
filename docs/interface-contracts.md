# Interface Contracts

Verbindliche Schnittstellen zwischen den Komponenten. Stand: entspricht dem deployten Chart
`charts/mes-pipeline` und dem Code in `ingestion/`, `stream-processing/`, `serving-api/`, `ui/`.

## Datenfluss

```
Ingestion (a/b/c) -> Kafka topic machine-events -> Stream Processing (a/b/c) -> MinIO -> Serving-API -> UI
                                                                                  ^
                                                                    CronJobs (Kompaktierung)
```

## Event-Schema (Kafka, JSON)

```json
{
  "timestamp": "ISO-8601 mit Zeitzone",
  "machine_id": "string",
  "machine_type": "A | B | C",
  "measurements": {
    "temperature": 0.0,
    "pressure": 0.0,
    "vibration": 0.0,
    "rotation_speed": 0.0,
    "power_consumption": 0.0,
    "runtime_seconds": 0.0,
    "status": "string"
  },
  "schema_version": "1.0"
}
```

Alle Felder unter `measurements` sind optional — jeder Maschinentyp liefert nur einen Teil:

| Typ | Felder in `measurements` | Statuswerte |
|---|---|---|
| A | `status`, `temperature`, `pressure`, `rotation_speed`, `power_consumption`, `runtime_seconds` | `OFF`, `STARTING`, `RUNNING`, `COOLING`, `ERROR` |
| B | `temperature`, `vibration` | — |
| C | `temperature`, `status` | `RUNNING`, `PAUSED` |

Neue Messfelder dürfen ohne Absprache in `measurements` ergänzt werden (Bronze übernimmt sie
automatisch). Umbenennungen bestehender Felder brauchen eine Änderung in
`stream-processing/streaming_job.py` und sind vorher im Team anzukündigen.

## Kafka

| Vertrag | Wert |
|---|---|
| Bootstrap-Server | `kafka:9092` |
| Topic | `machine-events` (hartkodiert in `ingestion/main.py` und `streaming_job.py`) |
| Partitionen | 3 (`KAFKA_NUM_PARTITIONS`, Auto-Create beim ersten Produce) |
| Replication-Factor | 1 |
| Key | `machine_id`; bekannte Maschinen werden explizit einer Partition zugeordnet (`A-001` → 0, `B-001` → 1, `C-001` → 2) |
| Retention | 168 h (Broker-Default) |

## MinIO

| Vertrag | Wert |
|---|---|
| Endpoint | `http://minio:9000` (Konsole `:9001`) |
| Buckets | `mes-data` (fachlich), `spark-checkpoints` (Checkpoints) — beide per Helm-Hook-Job angelegt |
| Bronze | `mes-data/bronze/machine-events/machine_type=<X>/event_date=<D>/`, Parquet, append; Schreibpfad je Instanz ist `…/machine_type=<X>/` |
| Silver-Metrics | `mes-data/silver/machine-metrics/machine_type=<X>/event_date=<D>/`, Parquet, append; Schreibpfad je Instanz ist `…/machine_type=<X>/` |
| Silver-Status | `mes-data/silver/machine-status/machine_type=<X>/`, Parquet, overwrite des eigenen Ordners |
| Checkpoints | `s3a://spark-checkpoints/checkpoints/<query>-<instanz>` (Bronze: `machine-events-v2-<instanz>`) |
| Kompaktierung | `silver-compaction` alle 30 min, `bronze-compaction` alle 10 min, jeweils nur die heutige Partition |

## Stream Processing

| Vertrag | Wert |
|---|---|
| Instanzen | `stream-processing-a/b/c`, Filter `machine_type IN MACHINE_TYPES` |
| Fenster | 10 s Tumbling über Event-Time `timestamp` |
| Watermark | 30 s (nur Silver-Metrics; Bronze ohne Watermark) |
| Grenzwert | `TEMP_LIMIT` aus ConfigMap, als Spalte `temperature_limit` mitgeschrieben |

## Serving-API

| Vertrag | Wert |
|---|---|
| Port / Service | 8000 / `serving-api` (ClusterIP) |
| `GET /health` | Liveness: Prozess läuft |
| `GET /ready` | Readiness: liest `silver/machine-status` aus MinIO, 503 bei Fehler |
| `GET /metrics/latest` | neuestes Fenster je Maschine, liest Partitionen ab gestern |
| `GET /metrics/history?machine_id=<id>&minutes=<n>` | Zeitreihe, Default `minutes=15`; ab `minutes > 120` serverseitig verdichtet (5 min / 15 min / 1 h Buckets) |
| Fehler | 503 mit `detail`, wenn MinIO nicht lesbar; leere Liste `[]`, wenn keine Daten |

### Antwortschema (`/metrics/latest`, je Objekt in `/metrics/history`)

```json
{
  "machine_id": "string",
  "machine_type": "string",
  "window_start": "ISO-8601",
  "window_end": "ISO-8601",
  "avg_temperature": 0.0,
  "min_temperature": 0.0,
  "max_temperature": 0.0,
  "event_count": 0,
  "last_status": "string | null",
  "temperature_limit": 0.0,
  "limit_exceeded": true
}
```

`NaN` aus Parquet wird zu `null`.

## UI

| Vertrag | Wert |
|---|---|
| Port / Service | 8050 / `ui` (NodePort 30080 in minikube, ClusterIP + Ingress in der DHBW Cloud) |
| Health | `GET /health` → `{"status": "ok"}` |
| Backend | `API_BASE_URL=http://serving-api:8000`, `USE_MOCK=false` |
| Seiten | `/`, `/machine/<id>`, `/messwerte` |
| Polling | alle `POLL_INTERVAL_SECONDS` (5 s), Verlauf über `HISTORY_MINUTES` (15) |

UI spricht ausschließlich mit der Serving-API. Kein direkter Zugriff auf Ingestion, Kafka oder MinIO.

## Ingestion

| Vertrag | Wert |
|---|---|
| Instanzen | `ingestion-a/b/c`, je `MACHINE_TYPES=A|B|C` |
| Kafka-Ziel | Topic `machine-events` |
| Takt | `INGESTION_INTERVAL_SECONDS` (2 s), je Durchlauf 10/5/3 Events (A/B/C) |

## Zugriff von außen

| Komponente | minikube | DHBW Cloud | Extern erreichbar |
|---|---|---|---|
| UI | NodePort 30080 | Ingress + TLS (`mes-ui.<kennung>.users.dhbw.site`) | ja |
| Serving-API | ClusterIP | ClusterIP | nein (nur über Port-Forward) |
| MinIO-Konsole | ClusterIP | ClusterIP | nein (nur Port-Forward / temporärer Tunnel) |
| Kafka | Headless | Headless | nein |

## Authentifizierung

Keine, in keiner Komponente (außer MinIO-Root-Credentials aus dem Secret).

## ConfigMap `pipeline-config`

| Schlüssel | Wert | Gelesen von |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Ingestion, Stream Processing |
| `TEMP_LIMIT` | `85` | Stream Processing |
| `MINIO_ENDPOINT` | `http://minio:9000` | Stream Processing, Kompaktierung, Serving-API |
| `MINIO_DATA_BUCKET` | `mes-data` | Stream Processing, Kompaktierung, Serving-API, Bucket-Job |
| `SPARK_CHECKPOINT_BUCKET` | `spark-checkpoints` | Bucket-Job |
| `SPARK_CHECKPOINT_DIR` | `s3a://spark-checkpoints/checkpoints` | Stream Processing |
| `SILVER_TABLE_PATH`, `STATUS_TABLE_PATH` | `silver/machine-metrics`, `silver/machine-status` | Stream Processing, Serving-API |
| `S3_CONNECT_TIMEOUT`, `S3_READ_TIMEOUT`, `S3_RETRY_MAX_ATTEMPTS`, `S3_USE_LISTINGS_CACHE`, `S3_CACHE_TTL_SECONDS` | `5`, `25`, `1`, `false`, `30` | Serving-API |
| `USE_MOCK`, `API_BASE_URL`, `API_TIMEOUT_SECONDS`, `CACHE_TTL_SECONDS` | `false`, `http://serving-api:8000`, `10`, `3` | UI |
| `POLL_INTERVAL_SECONDS`, `HISTORY_MINUTES` | `5`, `15` | UI |

## Secret `minio-credentials`

| Schlüssel | Für |
|---|---|
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | MinIO-Server, Bucket-Job |
| `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Stream Processing, Kompaktierung, Serving-API |

## Aggregat-Schema (Parquet, `silver/machine-metrics/`)

| Feld | Typ | Bedeutung |
|---|---|---|
| `window_start`, `window_end` | timestamp | 10-s-Fenster, Watermark 30 s |
| `machine_id`, `machine_type` | string | Gruppierung; `machine_type` Partitionsspalte |
| `avg_temperature`, `min_temperature`, `max_temperature` | double, nullable | |
| `event_count` | long | alle Events im Fenster |
| `last_status` | string, nullable | letzter Status im Fenster (`null` bei Typ B) |
| `temperature_limit` | double | geltender Grenzwert |
| `limit_exceeded` | boolean | `max_temperature > temperature_limit` |
| `event_date` | date | Partitionsspalte |
