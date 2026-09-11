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


@router.get("/history")
def metrics_history(machine_id: str, minutes: int = 15):
    """
    Liefert die Zeitreihe einer Maschine für die letzten `minutes` Minuten.

    Einsatz:
    --------
    Die UI zeigt einen Chart, der die Temperaturentwicklung einer Maschine
    über die letzten Minuten darstellt.

    Partition Pruning:
    -------------------
    `minutes` bestimmt den Cutoff-Zeitpunkt; daraus leiten wir das
    fruehestmoegliche `event_date` ab und lesen nur Partitionen ab diesem
    Tag (statt der gesamten Historie).
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

    return [row_to_json(row) for _, row in filtered.iterrows()]