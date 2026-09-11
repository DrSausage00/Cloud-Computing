import random
from datetime import UTC, datetime


RUN_DURATION_SECONDS = 60 * 60
PAUSE_DURATION_SECONDS = 5 * 60

AMBIENT_TEMPERATURE = 22.0
TARGET_TEMPERATURE = 85.0


machine_states = {}


def _get_initial_state():
    now = datetime.now(UTC)

    return {
        "temperature": AMBIENT_TEMPERATURE,
        "status": "RUNNING",
        "phase_started_at": now,
        "last_update": now,
    }


def generate_machine_c(machine_id: str = "C-001") -> dict:
    if machine_id not in machine_states:
        machine_states[machine_id] = _get_initial_state()

    state = machine_states[machine_id]

    now = datetime.now(UTC)

    elapsed = max(
        0.0,
        (now - state["last_update"]).total_seconds()
    )

    phase_duration = (
        now - state["phase_started_at"]
    ).total_seconds()

    state["last_update"] = now

    if state["status"] == "RUNNING":
        # Temperatur nähert sich langsam dem Betriebswert
        state["temperature"] += (
            TARGET_TEMPERATURE - state["temperature"]
        ) * 0.01 * elapsed

        # kleines Messrauschen
        state["temperature"] += random.uniform(-0.05, 0.05)

        if phase_duration >= RUN_DURATION_SECONDS:
            state["status"] = "PAUSED"
            state["phase_started_at"] = now

    elif state["status"] == "PAUSED":
        # Während der Pause kühlt die Maschine ab
        state["temperature"] += (
            AMBIENT_TEMPERATURE - state["temperature"]
        ) * 0.02 * elapsed

        state["temperature"] += random.uniform(-0.03, 0.03)

        if phase_duration >= PAUSE_DURATION_SECONDS:
            state["status"] = "RUNNING"
            state["phase_started_at"] = now

    return {
        "timestamp": now.isoformat(),
        "machine_id": machine_id,
        "status": state["status"],
        "temperature": round(state["temperature"], 2)
    }