# Stream Processing

Dieses Modul implementiert die Stream-Processing-Komponente der MES-Big-Data-Pipeline mit Apache Spark Structured Streaming.

## Aktueller Funktionsumfang

Die aktuelle Implementierung umfasst:

- Einlesen normalisierter Maschinendaten aus Kafka
- Parsen der JSON-Nachrichten in strukturierte Spark-Spalten
- Verarbeitung anhand der Event-Zeit
- Behandlung verspäteter Daten mittels Watermark
- 10-Sekunden-Fenster pro Maschine
- Berechnung der durchschnittlichen Temperatur
- Berechnung der minimalen und maximalen Temperatur
- Zählen der Events pro Zeitfenster
- Verarbeitung des Maschinenstatus
- Konfigurierbarer Temperaturgrenzwert
- Erkennung von Temperaturgrenzwertüberschreitungen
- Spark Checkpointing

## Aktueller Datenfluss

Der aktuelle lokale Datenfluss sieht folgendermaßen aus:

```text
Maschinensimulatoren
        ↓
Ingestion / Normalisierung
        ↓
Kafka
Topic: machine-events
        ↓
Spark Structured Streaming
        ↓
Watermark / Windowing / Aggregation
        ↓
Grenzwertprüfung
        ↓
Konsolenausgabe
```

Die Speicherung der verarbeiteten Daten im Data Lake auf MinIO wird in einem späteren Entwicklungsschritt ergänzt.

## Konfiguration

Der Temperaturgrenzwert kann über die Umgebungsvariable `TEMP_LIMIT` konfiguriert werden.

Wird kein Wert angegeben, verwendet der Streaming Job standardmäßig:

```text
95.0
```

Beispiel in PowerShell:

```powershell
$env:TEMP_LIMIT="95"
```

## Streaming Job lokal starten

### 1. In den Stream-Processing-Ordner wechseln

```powershell
cd stream-processing
```

### 2. Virtuelle Python-Umgebung erstellen

Falls noch keine virtuelle Umgebung vorhanden ist:

```powershell
python -m venv .venv
```

Anschließend aktivieren:

```powershell
.\.venv\Scripts\Activate.ps1
```

### 3. Abhängigkeiten installieren

```powershell
pip install -r requirements.txt
```

### 4. Python-Interpreter in VS Code auswählen

1. `Strg + Shift + P`
2. `Python: Select Interpreter`
3. `.venv\Scripts\python.exe` auswählen
4. Falls nötig: `Strg + Shift + P` → `Developer: Reload Window`

### 5. PySpark-Python-Umgebung festlegen

Damit Spark die virtuelle Python-Umgebung verwendet:

```powershell
$env:PYSPARK_PYTHON="$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="$PWD\.venv\Scripts\python.exe"
```

Die installierte PySpark-Version kann anschließend überprüft werden:

```powershell
python -c "import pyspark; print(pyspark.__version__)"
```

Aktuell wird PySpark 4.2.0 verwendet.

### 6. Java prüfen

```powershell
java -version
```

Für die lokale Entwicklung wird Java 17 verwendet.

## Kafka und Ingestion starten

Der Streaming Job liest Maschinendaten aus Kafka.

Kafka ist lokal unter folgendem Bootstrap-Server erreichbar:

```text
localhost:9092
```

Verwendetes Kafka-Topic:

```text
machine-events
```

### 7. Kafka mit Podman starten

In den Ingestion-Ordner wechseln:

```powershell
cd ..\ingestion
```

Kafka und die Initialisierung des Kafka-Topics starten:

```powershell
podman-compose up -d kafka kafka-init
```

Prüfen, ob der Kafka-Container läuft:

```powershell
podman ps
```

### 8. Ingestion starten

Die Ingestion simuliert Maschinendaten, normalisiert diese und schreibt die Events in das Kafka-Topic `machine-events`.

Die Ingestion wird in einem separaten Terminal gestartet:

```powershell
cd ingestion
python .\main.py
```

Falls sich das Terminal bereits im Ordner `stream-processing` befindet:

```powershell
cd ..\ingestion
python .\main.py
```

Das Terminal muss während der Ausführung des Streaming Jobs geöffnet bleiben.

## Spark Streaming Job starten

### 9. Neues Terminal öffnen

In den Stream-Processing-Ordner wechseln:

```powershell
cd stream-processing
```

Virtuelle Umgebung aktivieren:

```powershell
.\.venv\Scripts\Activate.ps1
```

Falls notwendig, die PySpark-Python-Umgebung erneut setzen:

```powershell
$env:PYSPARK_PYTHON="$PWD\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON="$PWD\.venv\Scripts\python.exe"
```

### 10. Streaming Job starten

Da Spark für die Kommunikation mit Kafka einen zusätzlichen Connector benötigt, wird der Job mit `spark-submit` gestartet:

```powershell
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 .\streaming_job.py
```

Der Kafka-Connector wird dabei automatisch von Spark eingebunden.

Der Streaming Job kann mit

```text
Strg + C
```

beendet werden.

## Verarbeitung

Die Kafka-Nachrichten werden zunächst aus JSON in strukturierte Spark-Spalten umgewandelt.

Anschließend werden die Events anhand ihrer Event-Zeit verarbeitet.

Pro Maschine wird ein 10-Sekunden-Zeitfenster gebildet.

Innerhalb eines Fensters werden folgende Werte berechnet:

- durchschnittliche Temperatur (`avg_temperature`)
- minimale Temperatur (`min_temperature`)
- maximale Temperatur (`max_temperature`)
- Anzahl der Events (`event_count`)
- letzter Maschinenstatus (`last_status`)

Zusätzlich werden folgende Informationen erzeugt:

- Beginn des Zeitfensters (`window_start`)
- Ende des Zeitfensters (`window_end`)
- Temperaturgrenzwert (`temperature_limit`)
- Grenzwertüberschreitung (`limit_exceeded`)

Eine Watermark von 20 Sekunden wird verwendet, um verspätete Events bei der zustandsbehafteten Verarbeitung zu berücksichtigen.

## Aktuelle Ausgabe

Die verarbeiteten Daten werden aktuell zu Testzwecken in der Konsole ausgegeben.

Beispiel:

```text
machine_id        C-001
machine_type      C
avg_temperature   89.4
min_temperature   82.1
max_temperature   97.3
event_count       5
last_status       RUNNING
temperature_limit 95.0
limit_exceeded    true
```

Wenn `max_temperature` größer als `temperature_limit` ist, wird `limit_exceeded` auf `true` gesetzt.

Die Konsolenausgabe wird später durch die Speicherung der Silver-Daten im Data Lake auf MinIO ersetzt.

## Windows-Hinweis

Für Apache Spark unter Windows können zusätzlich folgende Komponenten notwendig sein:

- `winutils.exe`
- `hadoop.dll`
- gesetzte Umgebungsvariable `HADOOP_HOME`

Diese Komponenten werden nur für die lokale Windows-Entwicklungsumgebung benötigt und werden nicht im Git-Repository gespeichert.