"""
Endpunkte, die genau das liefern, was die UI erwartet:
- aktuellster Messwert pro Maschine
- Zeitreihe einer Maschine über die letzten n Minuten
"""

from datetime import UTC, datetime, timedelta

from fastapi import APIRouter, HTTPException

from ..storage import load_table
from ..utils import row_to_json

router = APIRouter(prefix="/metrics", tags=["metrics"])


@router.get("/latest")
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

    Partition Pruning:
    -------------------
    Normalerweise reicht die heutige Partition. Edge Case: Eine Maschine hat
    seit gestern nichts mehr gemeldet -> ohne die Vortags-Partition wuerde
    sie komplett aus der Liste fallen. Deshalb "heute - 1 Tag" als Cutoff.
    """
    cutoff_date = (datetime.now(UTC) - timedelta(days=1)).date()

    try:
        df = load_table(event_date_filter=cutoff_date)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO nicht lesbar: {exc}")

    if df.empty:
        return []

    newest_index_per_machine = df.groupby("machine_id")["window_end"].idxmax()
    latest_rows = df.loc[newest_index_per_machine]

    return [row_to_json(row) for _, row in latest_rows.iterrows()]


def _downsample(df: pd.DataFrame, minutes: int) -> pd.DataFrame:
    """
    Verdichtet lange Zeitraeume auf groebere Zeitfenster.

    Grund: Bei 10-Sekunden-Fenstern kommen bei einem Monat Zeitraum
    ca. 260.000 Zeilen pro Maschine zusammen - das ueberlastet JSON-Transfer,
    Plotly und den pandas-DataFrame beim Client. Ab einer gewissen Groesse
    werden die Rohdaten daher zu groesseren Zeitfenstern gemittelt, bevor
    sie rausgehen.
    """
    if minutes <= 120 or df.empty:
        return df

    if minutes <= 2880:
        bucket = "5min"
    elif minutes <= 10080:
        bucket = "15min"
    else:
        bucket = "1h"

    indexed = df.set_index("window_start")
    weights = indexed["event_count"]

    resampled = pd.DataFrame({
        "avg_temperature": (indexed["avg_temperature"] * weights).resample(bucket).sum() / weights.resample(bucket).sum(),
        "min_temperature": indexed["min_temperature"].resample(bucket).min(),
        "max_temperature": indexed["max_temperature"].resample(bucket).max(),
        "event_count": weights.resample(bucket).sum(),
        "limit_exceeded": indexed["limit_exceeded"].resample(bucket).max(),
        "last_status": indexed["last_status"].resample(bucket).last(),
    }).dropna(subset=["avg_temperature"])

    resampled["window_start"] = resampled.index
    resampled["window_end"] = resampled.index + pd.Timedelta(bucket)
    resampled["machine_id"] = df["machine_id"].iloc[0]
    resampled["machine_type"] = df["machine_type"].iloc[0]
    resampled["temperature_limit"] = df["temperature_limit"].iloc[0]

    return resampled.reset_index(drop=True)


@router.get("/history")
def metrics_history(machine_id: str, minutes: int = 15):
    """
    ... (unveraendert bis auf die letzten zwei Zeilen)
    """
    cutoff = datetime.now(UTC) - timedelta(minutes=minutes)
    cutoff_date = cutoff.date()

    try:
        df = load_table(event_date_filter=cutoff_date)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"MinIO nicht lesbar: {exc}")

    if df.empty:
        return []

    filtered = df[
        (df["machine_id"] == machine_id) & (df["window_start"] >= cutoff)
    ].sort_values("window_start")

    filtered = _downsample(filtered, minutes)

    return [row_to_json(row) for _, row in filtered.iterrows()]