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

# --- Konfiguration: woher kommen die Daten? -----------------------------
# Alles ueber Umgebungsvariablen, damit jeder Tester nur seine eigenen
# MinIO-Zugangsdaten eintragen muss, ohne Code zu aendern.
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_DATA_BUCKET = os.getenv("MINIO_DATA_BUCKET", "mes-data")

# Genau der Pfad, unter dem der Spark-Job schreibt (silver_path in streaming_job.py)
SILVER_PATH = f"s3://{MINIO_DATA_BUCKET}/silver/machine-metrics/*/*.parquet"


def load_table() -> pd.DataFrame:
    """Liest alle Parquet-Dateien der Silver-Schicht als eine Tabelle ein.

    pandas kann direkt aus S3-kompatiblem Speicher lesen, wenn man ihm
    die Zugangsdaten mitgibt (storage_options).
    """
    df = pd.read_parquet(
        SILVER_PATH,
        storage_options={
            "key": MINIO_ACCESS_KEY,
            "secret": MINIO_SECRET_KEY,
            "client_kwargs": {"endpoint_url": MINIO_ENDPOINT},
        },
    )
    # window_start/window_end kommen aus Parquet als Datums-Objekte.
    # pandas.to_datetime stellt sicher, dass wir wirklich mit echten
    # Zeitstempeln rechnen koennen (fuer den Minuten-Filter unten).
    df["window_start"] = pd.to_datetime(df["window_start"], utc=True)
    df["window_end"] = pd.to_datetime(df["window_end"], utc=True)
    return df


def row_to_json(row: pd.Series) -> dict:
    """Eine Tabellenzeile -> ein JSON-taugliches dict fuer die UI.
    """
    return {
        "machine_id": row["machine_id"],
        "machine_type": row["machine_type"],
        "window_start": row["window_start"].isoformat(),
        "window_end": row["window_end"].isoformat(),
        "avg_temperature": float(row["avg_temperature"]),
        "min_temperature": float(row["min_temperature"]),
        "max_temperature": float(row["max_temperature"]),
        "event_count": int(row["event_count"]),
        "last_status": row["last_status"],
        "temperature_limit": float(row["temperature_limit"]),
        "limit_exceeded": bool(row["limit_exceeded"]),
    }


@app.get("/metrics/latest")
def metrics_latest():
    """Fuer die UI-Kacheln: das neueste Zeitfenster je Maschine."""
    try:
        df = load_table()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO nicht lesbar: {exc}")

    if df.empty:
        return []

    # Pro machine_id die Zeile mit dem groessten window_end behalten.
    newest_index_per_machine = df.groupby("machine_id")["window_end"].idxmax()
    latest_rows = df.loc[newest_index_per_machine]

    return [row_to_json(row) for _, row in latest_rows.iterrows()]


@app.get("/metrics/history")
def metrics_history(machine_id: str, minutes: int = 15):
    """Fuer den UI-Chart: Zeitreihe einer Maschine der letzten `minutes` Minuten."""
    try:
        df = load_table()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO nicht lesbar: {exc}")

    if df.empty:
        return []

    cutoff = datetime.now(UTC) - timedelta(minutes=minutes)
    is_right_machine = df["machine_id"] == machine_id
    is_recent_enough = df["window_start"] >= cutoff
    matching_rows = df[is_right_machine & is_recent_enough].sort_values("window_start")

    return [row_to_json(row) for _, row in matching_rows.iterrows()]