# Interface Contracts

## Datenfluss

```
UI (Datenlieferant, optional)
Ingestion -> Kafka topic machine-events -> Stream Processing -> MinIO -> Serving-API -> UI
```

## Event-Schema (Kafka, JSON)

```json
{
  "timestamp": "string",
  "machine_id": "string",
  "machine_type": "string",
  "measurements": {
    "temperature": 0.0,
    "pressure": 0.0,
    "vibration": 0.0,
    "rotation_speed": 0.0,
    "power_consumption": 0.0,
    "status": "string"
  },
  "schema_version": "string"
}
```

Alle Felder unter measurements nullable — jeder Maschinentyp liefert nur einen Teil davon
(A: temperature/pressure/rotation_speed/power_consumption; B: temperature/vibration;
C: temperature/status).

## Kafka (Kirill)

| Vertrag | Wert |
|---|---|
| Bootstrap-Server | `kafka:9092` |
| Topic | `machine-events` (hartkodiert in `ingestion/main.py`) |
| Partitionen | 3 |
| Key | `machine_id` |
| Retention | 168h |

## MinIO (Kirill)

| Vertrag | Wert |
|---|---|
| Endpoint | `http://minio:9000` |
| Bucket | `mes-data` |
| Pfad Aggregate | `mes-data/silver/machine-metrics/` |
| Format | Parquet, Append |
| Checkpoint | `s3a://spark-checkpoints/checkpoints` |
| Partitionierung | keine |

## Serving-API (Aaron)

| Vertrag | Wert |
|---|---|
| Port | 8000 |
| Service-Name | `serving-api` |
| `GET /metrics/latest` | aktuelle Werte |
| `GET /metrics/history?machine_id=<id>&minutes=<n>` | Zeitreihe, Default `minutes=15` |
| Health | keine Route |

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

## UI (Max)

| Vertrag | Wert |
|---|---|
| Port | 8050 |
| Health | `GET /health` |
| Backend | `http://serving-api:8000` |

## Ingestion (Leo)

| Vertrag | Wert |
|---|---|
| Kafka-Ziel | Topic `machine-events` |
| Sendeintervall | 2s (hartkodiert) |

## Routing (Lars)

```
UI -> Serving-API -> MinIO
```

UI spricht ausschließlich mit der Serving-API. Kein direkter Zugriff auf Ingestion, Kafka oder MinIO.

## Zugriff von außen

| Komponente | minikube | DHBW Cloud | Extern erreichbar |
|---|---|---|---|
| UI | NodePort 30080 | Ingress + TLS | ja |
| Serving-API | ClusterIP | ClusterIP | nein |
| MinIO-Konsole | ClusterIP | ClusterIP | nein |
| Kafka | ClusterIP | ClusterIP | nein |

## Authentifizierung

Keine, in keiner Komponente.
 
## ConfigMap `pipeline-config` (Lars)

| Schlüssel | Wert | Gelesen von |
|---|---|---|
| `KAFKA_BOOTSTRAP_SERVERS` | `kafka:9092` | Ingestion, Stream Processing |
| `TEMP_LIMIT` | `85` | Stream Processing |
| `SPARK_CHECKPOINT_DIR` | `s3a://spark-checkpoints/checkpoints` | Stream Processing |
| `SPARK_CHECKPOINT_BUCKET` | `spark-checkpoints` | Bucket-Job (Guide 07) |
| `MINIO_ENDPOINT` | `http://minio:9000` | Stream Processing, Serving-API |
| `MINIO_DATA_BUCKET` | `mes-data` | Stream Processing, Serving-API |
| `POLL_INTERVAL_SECONDS` | `5` | UI |
| `HISTORY_MINUTES` | `15` | UI |
| `USE_MOCK` | `false` | UI |
| `API_BASE_URL` | `http://serving-api:8000` | UI |

## Secret `minio-credentials` (Lars)

| Schlüssel | Für |
|---|---|
| `MINIO_ROOT_USER`, `MINIO_ROOT_PASSWORD` | MinIO-Server |
| `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY` | Stream Processing, Serving-API |

## Aggregat-Schema (Parquet, `silver/machine-metrics/`)

| Feld | Typ | Bedeutung |
|---|---|---|
| `window_start`, `window_end` | timestamp | 10s-Fenster, Watermark 20s |
| `machine_id`, `machine_type` | string | Gruppierung |
| `avg_temperature`, `min_temperature`, `max_temperature` | double, nullable | |
| `event_count` | long | alle Events im Fenster |
| `temperature_limit` | double | geltender Grenzwert |
| `limit_exceeded` | boolean | `max_temperature > temperature_limit` |
| `last_status` | string, nullable | letzter Status im Fenster |
