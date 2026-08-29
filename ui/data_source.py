"""
Einzige Stelle, an der die UI Daten holt.

Solange Serving-API noch nicht steht, liefert dieses Modul noch
erfundene Daten im Format der Tabelle `silver_machine_metrics`
(siehe ingestion/storage/schema.sql).

Umstellung auf die echte API kommt spaeter.
"""

import os
import random
from datetime import datetime, timedelta, UTC

import requests

USE_MOCK = os.getenv("USE_MOCK", "true").lower() == "true"
API_BASE_URL = os.getenv("API_BASE_URL", "http://serving-api:8000")

WINDOW_SECONDS = 10
TEMPERATURE_LIMIT = 95.0

# Die drei Maschinen aus ingestion/simulators/
MACHINES = [
    {"machine_id": "A-001", "machine_type": "A", "base_temp": 75.0},
    {"machine_id": "B-001", "machine_type": "B", "base_temp": 65.0},
    {"machine_id": "C-001", "machine_type": "C", "base_temp": 85.0},
]

STATUS_CHOICES = ["RUNNING", "STOPPED", "ERROR"]

# Gedaechtnis des Mocks, damit die Charts über die Zeit plausibel aussehen
_history: list[dict] = []


def _build_row(machine: dict, window_start: datetime) -> dict:
    """Baut eine Zeile im Format von silver_machine_metrics."""
    avg = machine["base_temp"] + random.uniform(-6, 14)
    spread = random.uniform(0.5, 3.0)

    return {
        "machine_id": machine["machine_id"],
        "machine_type": machine["machine_type"],
        "window_start": window_start.isoformat(),
        "window_end": (window_start + timedelta(seconds=WINDOW_SECONDS)).isoformat(),
        "avg_temperature": round(avg, 2),
        "min_temperature": round(avg - spread, 2),
        "max_temperature": round(avg + spread, 2),
        "event_count": random.randint(4, 12),
        "last_status": random.choices(STATUS_CHOICES, weights=[85, 10, 5])[0],
        "temperature_limit": TEMPERATURE_LIMIT,
        "limit_exceeded": avg + spread > TEMPERATURE_LIMIT,
    }


def _seed_mock(minutes: int = 15) -> None:
    """Legt beim Start eine Vorgeschichte an, damit die Charts nicht leer sind."""
    if _history:
        return

    now = datetime.now(UTC)
    steps = (minutes * 60) // WINDOW_SECONDS

    for step in range(steps, 0, -1):
        window_start = now - timedelta(seconds=step * WINDOW_SECONDS)
        for machine in MACHINES:
            _history.append(_build_row(machine, window_start))


def _advance_mock() -> None:
    """Haengt ein neues Zeitfenster an, wenn genug Zeit vergangen ist."""
    now = datetime.now(UTC)
    last = datetime.fromisoformat(_history[-1]["window_start"])

    if (now - last).total_seconds() < WINDOW_SECONDS:
        return

    for machine in MACHINES:
        _history.append(_build_row(machine, now))

    # Mock haelt nur die letzte Stunde vor
    cutoff = now - timedelta(hours=1)
    while datetime.fromisoformat(_history[0]["window_start"]) < cutoff:
        _history.pop(0)


def fetch_latest() -> list[dict]:
    """Neuestes Zeitfenster je Maschine. Fuer die Live-Kacheln."""
    if USE_MOCK:
        _seed_mock()
        _advance_mock()

        newest: dict[str, dict] = {}
        for row in _history:
            newest[row["machine_id"]] = row
        return list(newest.values())

    response = requests.get(f"{API_BASE_URL}/metrics/latest", timeout=5)
    response.raise_for_status()
    return response.json()


def fetch_history(machine_id: str, minutes: int = 15) -> list[dict]:
    """Zeitreihe einer Maschine. Fuer den Verlaufs-Chart."""
    if USE_MOCK:
        _seed_mock()
        cutoff = datetime.now(UTC) - timedelta(minutes=minutes)
        return [
            row
            for row in _history
            if row["machine_id"] == machine_id
            and datetime.fromisoformat(row["window_start"]) >= cutoff
        ]

    response = requests.get(
        f"{API_BASE_URL}/metrics/history",
        params={"machine_id": machine_id, "minutes": minutes},
        timeout=5,
    )
    response.raise_for_status()
    return response.json()
