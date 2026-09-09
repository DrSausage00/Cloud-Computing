"""
Hilfsfunktionen zur Umwandlung von DataFrame-Zeilen in JSON-taugliche
Strukturen für die UI.
"""

import pandas as pd


def safe_float(value):
    """
    Wandelt einen Wert in float um, aber ersetzt NaN durch None.

    Hintergrund:
    ------------
    JSON erlaubt keine NaN-Werte.
    Starlette/JSONResponse wirft sonst einen ValueError.
    """
    return float(value) if pd.notna(value) else None


def row_to_json(row: pd.Series) -> dict:
    """Eine Tabellenzeile -> ein JSON-taugliches dict fuer die UI."""
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
