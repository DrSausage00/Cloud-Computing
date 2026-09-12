# Storage Layer

## Überblick

Das Projekt nutzt MinIO als S3-kompatiblen Objektspeicher.

Fachliche Daten liegen im Bucket `mes-data`. Spark-Structured-Streaming-Checkpoints liegen
getrennt davon im Bucket `spark-checkpoints`, damit ein Zurücksetzen der fachlichen Daten nicht
versehentlich den Wiederanlauf-Zustand der Streams berührt (und umgekehrt).

## Speicherstruktur

```
mes-data/
├── bronze/machine-events/       # Rohereignisse, partitioniert nach event_date
└── silver/
    ├── machine-metrics/         # 10s-Aggregate, partitioniert nach machine_type/event_date
    └── machine-status/          # letzter bekannter Status je Maschine, partitioniert nach machine_type

spark-checkpoints/
└── checkpoints/
    ├── machine-events-<a|b|c>/  # Bronze-Checkpoint je Stream-Processing-Instanz
    ├── machine-metrics-<a|b|c>/ # Silver-Metrics-Checkpoint je Instanz
    └── machine-status-<a|b|c>/  # Silver-Status-Checkpoint je Instanz
```

## Bronze-Schicht

Bronze enthält die normalisierten Maschinen-Ereignisse aus dem Kafka-Topic `machine-events`.
Die ursprünglichen CSV-, JSON- und Pipe-separierten Quellformate sind bereits von der Ingestion
normalisiert, bevor sie nach Kafka geschrieben werden — Bronze enthält also die kanonischen
Kafka-Events, nicht die ursprünglichen Rohstrings.

- Ort: `s3a://mes-data/bronze/machine-events`
- Partitionierung: `event_date`
- Geschrieben von Sparks nativem Structured-Streaming-File-Sink (append-only)

Bronze ist bewusst eine **zusätzliche, dauerhafte Ablage über die 7 Tage Kafka-Retention
hinaus** (siehe Haupt-README §3/§12) — nicht redundant zu Kafka, sondern die Antwort auf die
dort benannte Lücke, dass die Rohhistorie nach 7 Tagen sonst unwiederbringlich weg wäre.

## Silver-Schicht

Silver enthält die von Stream Processing erzeugten, verarbeiteten Maschinenmetriken:
Fenster-Aggregationen, zustandsbehaftete Information (letzter Status) und Grenzwertprüfung.

**`silver/machine-metrics`**
- Ort: `s3a://mes-data/silver/machine-metrics`
- Partitionierung: `machine_type`, `event_date`

**`silver/machine-status`**
- Ort: `s3a://mes-data/silver/machine-status`
- Partitionierung: `machine_type`

## Tabellenformat

**Parquet**, kein Delta Lake/Iceberg. Die physischen Dateien sind spaltenorientiertes,
komprimiertes Parquet mit eingebettetem Schema. Ohne Table-Format-Schicht gibt es dafür keine
ACID-Transaktionen und kein Time Travel — eine bewusste, im Haupt-README §12 benannte Lücke,
kein technisches Versehen.

## Partitionierungsentscheidung

Beide fachlichen Tabellen sind nach `machine_type` partitioniert (bei `machine-metrics`
zusätzlich nach `event_date`). Zwei unabhängige Gründe:

1. **Zeitfenster-Abfragen.** MES-Auswertungen fragen typischerweise nach Zeiträumen. Eine
   Partitionierung nach `event_date` erlaubt der Serving-API, irrelevante Partitionen beim
   Lesen komplett zu überspringen (Partition Pruning), statt die gesamte Historie zu scannen.
2. **Parallele Schreiber.** Drei Stream-Processing-Instanzen (siehe Haupt-README §8) schreiben
   gleichzeitig, je eine pro Maschinentyp. Ohne Partitionierung nach `machine_type` würde
   `mode("overwrite")` bei jedem Schreibvorgang einer Instanz die Daten der jeweils anderen
   beiden überschreiben, statt nur die eigene Partition zu ersetzen
   (`spark.sql.sources.partitionOverwriteMode = dynamic` macht das möglich).

Bewusst **nicht** nach `machine_id` partitioniert: Bei potenziell vielen Maschinen entstünde
eine sehr große Zahl kleiner Partitionen. `machine_type` hat nur drei Ausprägungen im Prototyp
und deckt trotzdem den Konflikt zwischen den parallelen Schreibern vollständig ab.

## Small-Files-Problem und Kompaktierung

Jeder Streaming-Micro-Batch erzeugt eine neue Datei. Über mehrere Stunden Laufzeit entstehen so
pro Partition schnell Zehntausende kleiner Dateien (beobachtet: über 70.000 Objekte in Bronze
nach rund einem Tag), was Lesezugriffe spürbar verlangsamt. Zwei periodische `CronJob`s lesen
die aktuelle Partition komplett neu ein und schreiben sie als eine einzige, große Datei zurück
— Details, Zeitplan und zwei dabei aufgetretene, nicht-triviale Fehlerbilder (Sparks
`_spark_metadata`-Log bei Bronze, S3-Commit-Races bei parallelem Schreiben) stehen im
Haupt-README §6.
