# Stream Processing

Dieses Modul implementiert die Stream-Processing-Komponente der MES-Big-Data-Pipeline mit Apache Spark Structured Streaming.

Einordnung in die Gesamtarchitektur: siehe Haupt-README, §4 und §5.

## Funktionsumfang

- Einlesen normalisierter Maschinendaten aus Kafka
- Parsen der Kafka-Nachrichten (Envelope mit generischer `measurements`-Map) in strukturierte Spark-Spalten
- Filterung auf den/die Maschinentyp(en) dieser Instanz (`MACHINE_TYPES`) — Grundlage der horizontalen Skalierung
- Verarbeitung anhand der Event-Zeit, 10-Sekunden-Fenster je Maschine
- Behandlung verspäteter Daten mittels Watermark (30s)
- Aggregation: Durchschnitts-, Minimal-, Maximaltemperatur, Event-Zählung
- Zustandsbehaftete Verarbeitung des letzten bekannten Maschinenstatus (`last_status`)
- Anreicherung mit konfigurierbarem Temperaturgrenzwert aus der ConfigMap
- Schreiben von drei Ausgaben nach MinIO: Bronze (Rohereignisse), Silver-Metrics (Aggregate), Silver-Status
- Spark Checkpointing, isoliert je Instanz

## Datenfluss

```text
Kafka (Topic: machine-events, gefiltert auf MACHINE_TYPES dieser Instanz)
        ↓
Parsing (generische measurements-Map → benannte Spalten)
        ↓
        ├──▶ Bronze-Stream  ──▶ MinIO: bronze/machine-events        (Rohereignisse, append)
        ├──▶ Status-Stream  ──▶ MinIO: silver/machine-status        (foreachBatch, overwrite je machine_type-Partition)
        └──▶ Aggregation (Watermark, 10s-Fenster, avg/min/max/count)
                    ↓
             Silver-Stream  ──▶ MinIO: silver/machine-metrics       (foreachBatch, partitioniert nach machine_type/event_date)
```

Die drei Ausgaben laufen als unabhängige `writeStream`-Queries aus demselben Quell-DataFrame,
jede mit eigenem Checkpoint-Pfad (`checkpoint_dir/<name>-<machine_types_suffix>`) — damit die
drei parallelen Instanzen (`stream-processing-a/b/c`, siehe unten) sich nicht gegenseitig den
Zustand überschreiben.

## Horizontale Skalierung

Statt eines einzelnen Pods mit `.master("local[N]")`, der das gesamte Topic verarbeitet, laufen
drei Instanzen parallel — je eine pro Maschinentyp:

| Instanz | `MACHINE_TYPES` | Checkpoint-Suffix |
|---|---|---|
| `stream-processing-a` | `A` | `machine-metrics-a`, `machine-status-a`, `machine-events-a` |
| `stream-processing-b` | `B` | `machine-metrics-b`, `machine-status-b`, `machine-events-b` |
| `stream-processing-c` | `C` | `machine-metrics-c`, `machine-status-c`, `machine-events-c` |

**Warum nicht einfach `replicas: 3`.** Mehrere Replicas desselben Deployments würden dasselbe
Kafka-Topic vom selben Offset lesen und dieselben Aggregate mehrfach schreiben. Die
Aufteilung erfolgt stattdessen über `MACHINE_TYPES` als Filter — jede Instanz verarbeitet nur
ihren eigenen Maschinentyp, konfiguriert über den Helm-`range` in
[`charts/mes-pipeline/templates/stream-processing.yaml`](../charts/mes-pipeline/templates/stream-processing.yaml).

**Grenze:** Die Parallelität ist an die Anzahl der Kafka-Partitionen gekoppelt (hier 3). Mehr
Instanzen als Partitionen bringen keinen zusätzlichen Durchsatz.

## Konfiguration

| Variable | Standard | Bedeutung |
|---|---|---|
| `TEMP_LIMIT` | `95.0` | Temperaturgrenzwert, ab dem `limit_exceeded` gesetzt wird |
| `MACHINE_TYPES` | `A,B,C` | Kommaliste der Typen, die diese Instanz verarbeitet |
| `SILVER_TRIGGER_INTERVAL_SECONDS` | `10` | Trigger-Intervall des Silver-Schreib-Micro-Batches |
| `SPARK_CHECKPOINT_DIR` | `/checkpoints` | Basis-Pfad für alle Checkpoints dieser Instanz |
| `MINIO_ENDPOINT`, `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, `MINIO_DATA_BUCKET` | – | MinIO-Zugang |

## Kompaktierung

`compact.py` ist ein separates, batch-artiges Skript (kein Streaming-Job), das periodisch über
zwei Kubernetes-`CronJob`s läuft und die vielen kleinen, von den Streaming-Writes erzeugten
Parquet-Dateien einer Partition zu einer großen zusammenfasst. Details und Begründung: siehe
Haupt-README §6.

| Env-Variable | Bedeutung |
|---|---|
| `TABLE_PATH` | Zu kompaktierender Tabellenpfad, z. B. `silver/machine-metrics` oder `bronze/machine-events` |
| `PARTITION_COLS` | Partitionsspalten dieser Tabelle, kommasepariert |
| `COMPACT_EVENT_DATE` | Zu kompaktierender Tag, Default: heute |

## Docker-Image

Die Spark-/Hadoop-Pakete (`spark-sql-kafka-0-10`, `hadoop-aws`, AWS-SDK-Bundle, insgesamt
~700 MB) werden beim Image-Build vorab aufgelöst und im Image gecacht (`Dockerfile`), statt sie
bei jedem Container-Start erneut über `--packages` von Maven Central zu laden — das vermeidet
wiederholtes Rate-Limiting bei häufigen Neustarts (z. B. durch die alle 10 Minuten laufende
Bronze-Kompaktierung). Der Paket-Download-Layer steht im Dockerfile bewusst **vor** dem `COPY`
des Anwendungscodes, damit reine Code-Änderungen diesen großen Layer nicht neu bauen.

## Lokal starten (ohne Kubernetes)

```powershell
cd stream-processing
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

$env:PYSPARK_PYTHON="$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="$PWD\.venv\Scripts\python.exe"

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0,org.apache.hadoop:hadoop-aws:3.5.0 .\streaming_job.py
```

Voraussetzung: Kafka läuft lokal erreichbar unter `localhost:9092` mit Topic `machine-events`,
und die Ingestion (`../ingestion/main.py`) speist Events ein. Für Windows kann zusätzlich
`winutils.exe`/`hadoop.dll`/`HADOOP_HOME` nötig sein — nur für die lokale Entwicklungsumgebung,
nicht im Repository enthalten.
