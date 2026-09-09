MES Serving-API
Schlanke FastAPI-Anwendung, die die Silver-Schicht aus MinIO (Parquet, geschrieben vom Spark-Streaming-Job) einliest und über HTTP genau die Endpunkte bereitstellt, die die UI benötigt. Kein Cache, kein Delta-Lake, keine zusätzliche Business-Logik — die API liest und aggregiert nur beim Anfragezeitpunkt.

Einordnung in die Gesamtarchitektur: siehe Haupt-README.

Inhaltsverzeichnis
1. Projektstruktur
2. Voraussetzungen
3. Konfiguration (.env)
4. Lokal starten
5. Mit Docker starten
6. Endpunkte
7. Testen
8. Fehlerverhalten
1. Projektstruktur
serving-api/
├── app/
│   ├── main.py           # FastAPI-App, bindet nur die Router ein
│   ├── config.py         # Env-Variablen, MinIO-Settings
│   ├── storage.py        # load_table() – liest die Silver-Schicht aus MinIO
│   ├── utils.py           # safe_float(), row_to_json()
│   └── routers/
│       ├── health.py     # /health, /ready
│       └── metrics.py    # /metrics/latest, /metrics/history
├── Dockerfile
├── requirements.txt
├── .env.example
└── README.md
Datei/Modul	Verantwortlich für
app/config.py	Alle Env-Variablen und MinIO-Zugangsdaten an einer Stelle
app/storage.py	Parquet-Zugriff inkl. Retry bei transienten FileNotFoundError und Timeouts gegen MinIO
app/utils.py	Umwandlung von DataFrame-Zeilen in JSON-taugliche dicts (NaN → None)
app/routers/health.py	Liveness-/Readiness-Probes für Kubernetes
app/routers/metrics.py	Die eigentlichen Fachendpunkte für die UI
2. Voraussetzungen
Werkzeug	Zweck
Python 3.12	Laufzeitumgebung
Docker oder Podman	Containerbetrieb (optional für lokale Entwicklung)
Laufendes MinIO	Datenquelle – muss die Silver-Schicht unter mes-data/silver/machine-metrics bereitstellen
3. Konfiguration (.env)
Die API liest ihre Konfiguration ausschließlich aus Umgebungsvariablen, damit sie ohne Codeänderung lokal, in Podman und in Kubernetes läuft.

cp .env.example .env
# .env mit echten Werten füllen
Variable	Bedeutung	Default
MINIO_ENDPOINT	URL des MinIO-Servers	http://minio:9000
MINIO_ACCESS_KEY	Zugangsschlüssel	– (erforderlich)
MINIO_SECRET_KEY	Geheimer Schlüssel	– (erforderlich)
MINIO_DATA_BUCKET	Bucket-Name der Silver-Schicht	mes-data
.env.example darf ins Git, .env selbst niemals (steht in .gitignore/.dockerignore).

4. Lokal starten
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
pip install -r requirements.txt

export $(grep -v '^#' .env | xargs)   # oder .env per IDE/direnv laden
uvicorn app.main:app --reload --port 8000
Die API läuft danach unter http://localhost:8000.

5. Mit Docker starten
docker build -t mes-serving-api .

docker run --rm -p 8000:8000 --env-file .env mes-serving-api
In Kubernetes werden dieselben Variablen über eine ConfigMap/Secret statt --env-file gesetzt.

6. Endpunkte
Methode	Pfad	Beschreibung
GET	/health	Liveness-Probe – Prozess läuft, sagt nichts über MinIO aus
GET	/ready	Readiness-Probe – prüft, ob MinIO erreichbar und die Tabelle lesbar ist
GET	/metrics/latest	Neuestes Zeitfenster je Maschine (Basis für die Kachel-Übersicht)
GET	/metrics/history?machine_id=<id>&minutes=<n>	Zeitreihe einer Maschine der letzten n Minuten (Default 15), Basis für den Temperaturverlauf
Beispiel-Response (/metrics/latest, ein Eintrag):

{
  "machine_id": "A-001",
  "machine_type": "A",
  "window_start": "2026-09-09T10:00:00+00:00",
  "window_end": "2026-09-09T10:00:10+00:00",
  "avg_temperature": 78.3,
  "min_temperature": 76.1,
  "max_temperature": 80.4,
  "event_count": 12,
  "last_status": "RUNNING",
  "temperature_limit": 85.0,
  "limit_exceeded": false
}
7. Testen
FastAPI generiert automatisch eine interaktive Swagger-UI:

http://localhost:8000/docs
Dort lassen sich alle Endpunkte direkt im Browser aufrufen, ohne curl oder Postman.

8. Fehlerverhalten
Situation	Verhalten
MinIO nicht erreichbar	503 mit Fehlermeldung, sowohl bei /ready als auch bei /metrics/*
Parquet-Datei kurzzeitig ersetzt (Spark schreibt gerade)	Bis zu 3 automatische Wiederholungsversuche mit kurzer Pause, bevor ein Fehler zurückgegeben wird
Silver-Schicht leer	Endpunkte liefern eine leere Liste [], kein Fehler
NaN-Werte in Parquet (z. B. fehlende Vibrationsdaten)	Werden zu null in der JSON-Antwort, nicht zu einem Serverfehler