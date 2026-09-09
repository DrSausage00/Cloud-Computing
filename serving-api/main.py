"""
MES Serving-API

1. Liest die Parquet-Dateien, die Spark nach MinIO schreibt, in eine Tabelle ein.
2. Bietet zwei Endpunkte, die genau das liefern, was die UI erwartet.
3. Kein Cache, kein Delta-Lake, keine Kubernetes-Health-Checks.

Starten:
    uvicorn main:app --reload --port 8000

Testen im Browser:
    http://localhost:8000/docs
"""

import os
from datetime import datetime, timedelta, UTC

import pandas as pd
from fastapi import FastAPI, HTTPException

app = FastAPI(title="MES Serving API (schlank)")

# ----------------------------------------------------------------------
# KONFIGURATION
# ----------------------------------------------------------------------
# Die API liest Daten aus MinIO (S3-kompatibel). Alle Zugangsdaten werden
# über Umgebungsvariablen gesteuert, damit die API in verschiedenen
# Umgebungen (lokal, Podman, Kubernetes) ohne Codeänderung funktioniert.

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_DATA_BUCKET = os.getenv("MINIO_DATA_BUCKET", "mes-data")

# Genau der Pfad, unter dem der Spark-Job schreibt (silver_path in streaming_job.py)
SILVER_PATH = f"s3://{MINIO_DATA_BUCKET}/silver/machine-metrics/"

# Timeouts gegen MinIO:
# Ohne diese Timeouts kann die API hängen bleiben, wenn MinIO nicht antwortet.
# Das blockiert Uvicorn-Worker → API reagiert nicht mehr (kritischer Bug 0).
STORAGE_OPTIONS = {
    "key": MINIO_ACCESS_KEY,
    "secret": MINIO_SECRET_KEY,
    "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
    "config_kwargs": {
        "connect_timeout": 5,   # Verbindung muss innerhalb 5s stehen
        "read_timeout": 10      # Lesen muss innerhalb 10s erfolgen
    },
}

# ----------------------------------------------------------------------
# HILFSFUNKTIONEN
# ----------------------------------------------------------------------

def load_table() -> pd.DataFrame:
    """
    Liest die gesamte Silver-Schicht als DataFrame ein.
    Wichtige Aspekte:
    -----------------
    1. RETRY-MECHANISMUS
       Spark schreibt laufend neue Dateien in das Verzeichnis.
       Pandas/pyarrow listet zuerst alle Dateien auf und öffnet sie dann.
       Wenn Spark eine Datei zwischen diesen beiden Schritten ersetzt,
       entsteht ein FileNotFoundError → transienter Fehler.

       Lösung: 3 Versuche mit kurzer Pause.

    2. TIMEOUTS
       Wenn MinIO hängt, darf die API nicht blockieren.
       Die Timeouts sorgen dafür, dass die API nach spätestens 10s
       mit einem Fehler reagiert statt dauerhaft zu hängen.

    3. ZEITSTEMPEL-KONVERTIERUNG
       window_start/window_end müssen echte UTC-Timestamps sein,
       damit Filterung und Sortierung korrekt funktionieren.
    """
    last_exc = None

    for attempt in range(3):
        try:
            df = pd.read_parquet(SILVER_PATH, storage_options=STORAGE_OPTIONS)

            # Konvertierung der Zeitspalten
            df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
            df["window_end"] = pd.to_datetime(df["window_end"], utc=True)

            return df

        except FileNotFoundError as exc:
            # Transienter Fehler → kurz warten und erneut versuchen
            last_exc = exc
            time.sleep(0.5)

        except Exception as exc:
            # Andere Fehler sofort weitergeben (z. B. Authentifizierung)
            raise exc

    # Wenn alle Versuche scheitern → letzten Fehler zurückgeben
    raise last_exc

def safe_float(value):
    """
    Wandelt einen Wert in float um, aber ersetzt NaN durch None.

    Hintergrund:
    ------------
    JSON erlaubt keine NaN-Werte.
    Starlette/JSONResponse wirft sonst einen ValueError (Bug 2).
    """
    return float(value) if pd.notna(value) else None

def row_to_json(row: pd.Series) -> dict:
    """Eine Tabellenzeile -> ein JSON-taugliches dict fuer die UI.
    """
    return {
        "machine_id": row["machine_id"],
        "machine_type": row["machine_type"],
        "window_start": row["window_start"].isoformat(),
        "window_end": row["window_end"].isoformat(),
        "avg_temperature": safe_float(row["avg_temperature"]),
        "min_temperature": safe_float(row["min_temperature"]),
        "max_temperature": safe_float(row["max_temperature"]),
        "event_count": int(row["event_count"]),
        "last_status": row["last_status"],
        "temperature_limit": safe_float(row["temperature_limit"]),
        "limit_exceeded": bool(row["limit_exceeded"]),
    }

# ----------------------------------------------------------------------
# ENDPOINTS
# ----------------------------------------------------------------------

@app.get("/health")
def health():
    """
    Liveness-Probe:
    ----------------
    Zeigt nur an, dass der Prozess läuft und Anfragen beantwortet.
    Sagt NICHTS über MinIO oder Datenqualität aus.
    """
    return {"status": "ok"}

@app.get("/ready")
def ready():
    """
    Readiness-Probe:
    ----------------
    Prüft, ob die API wirklich Daten liefern kann:
    - MinIO erreichbar?
    - Parquet-Dateien lesbar?
    - Tabelle nicht korrupt?

    Kubernetes nutzt diesen Endpunkt, um Pods erst dann in den
    Load-Balancer aufzunehmen, wenn sie wirklich bereit sind.
    """
    try:
        df = load_table()
        return {"status": "ok", "rows": len(df)}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Datenquelle nicht erreichbar: {exc}")

@app.get("/metrics/latest")
def metrics_latest():
    """
    Liefert das neueste Zeitfenster je Maschine.

    Einsatz:
    --------
    Die UI zeigt pro Maschine eine Kachel mit:
    - Temperatur
    - Status
    - Limit-Überschreitung
    - Zeitfenster
    """
    try:
        df = load_table()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO nicht lesbar: {exc}")

    if df.empty:
        return []

    newest_index_per_machine = df.groupby("machine_id")["window_end"].idxmax()
    latest_rows = df.loc[newest_index_per_machine]

    return [row_to_json(row) for _, row in latest_rows.iterrows()]


@app.get("/metrics/history")
def metrics_history(machine_id: str, minutes: int = 15):
    """
    Liefert die Zeitreihe einer Maschine für die letzten `minutes` Minuten.

    Einsatz:
    --------
    Die UI zeigt einen Chart, der die Temperaturentwicklung einer Maschine
    über die letzten Minuten darstellt.
    """
    try:
        df = load_table()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO nicht lesbar: {exc}")

    if df.empty:
        return []

    cutoff = datetime.now(UTC) - timedelta(minutes=minutes)

    filtered = df[
        (df["machine_id"] == machine_id)
        & (df["window_start"] >= cutoff)
    ].sort_values("window_start")

    return [row_to_json(row) for _, row in filtered.iterrows()]
