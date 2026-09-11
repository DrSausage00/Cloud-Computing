import random
from datetime import UTC, datetime


machine_states = {}


def _get_initial_state():
    return {
        "temperature": 60.0,
        "vibration": 3.0
    }


def generate_machine_b(machine_id: str = "B-001") -> dict:
    if machine_id not in machine_states:
        machine_states[machine_id] = _get_initial_state()

    state = machine_states[machine_id]

    state["temperature"] += random.uniform(-0.2, 0.4)
    state["temperature"] = max(
        50.0,
        min(80.0, state["temperature"])
    )
    target_vibration = (
        2.0
        + 0.08 * (state["temperature"] - 50.0)
    )

    state["vibration"] += (
        target_vibration - state["vibration"]
    ) * 0.2
    state["vibration"] += random.uniform(-0.1, 0.1)

    state["vibration"] = max(
        0.0,
        min(10.0, state["vibration"])
    )

    return {
        "ts": datetime.now(UTC).isoformat(),
        "id": machine_id,
        "temp": round(state["temperature"], 2),
        "vibration": round(state["vibration"], 2)
    }