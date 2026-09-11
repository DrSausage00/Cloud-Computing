import os
import random
from datetime import UTC, datetime


OPERATING_START_HOUR = int(os.getenv("MACHINE_A_START_HOUR", "6"))
OPERATING_END_HOUR = int(os.getenv("MACHINE_A_END_HOUR", "18"))

AMBIENT_TEMPERATURE = float(
    os.getenv("MACHINE_A_AMBIENT_TEMPERATURE", "22.0")
)

COOLING_THRESHOLD = float(
    os.getenv("MACHINE_A_COOLING_THRESHOLD", "92.0")
)

COOLING_TARGET = float(
    os.getenv("MACHINE_A_COOLING_TARGET", "78.0")
)

ERROR_TEMPERATURE = float(
    os.getenv("MACHINE_A_ERROR_TEMPERATURE", "105.0")
)

ERROR_PRESSURE = float(
    os.getenv("MACHINE_A_ERROR_PRESSURE", "9.0")
)


machine_states = {}


def _get_initial_state():
    now = datetime.now(UTC)

    return {
        "temperature": AMBIENT_TEMPERATURE,
        "pressure": 0.0,
        "rotation_speed": 0.0,
        "power_consumption": 0.0,
        "runtime_seconds": 0.0,
        "status": "OFF",
        "last_update": now,
    }


def _is_operating_time(now: datetime) -> bool:
    return OPERATING_START_HOUR <= now.hour < OPERATING_END_HOUR


def _update_off(state, elapsed):
    state["status"] = "OFF"

    state["rotation_speed"] = max(
        0.0,
        state["rotation_speed"] - 300 * elapsed
    )

    state["pressure"] = max(
        0.0,
        state["pressure"] - 0.3 * elapsed
    )

    state["power_consumption"] = max(
        0.0,
        state["power_consumption"] - 1.0 * elapsed
    )

    if state["temperature"] > AMBIENT_TEMPERATURE:
        state["temperature"] = max(
            AMBIENT_TEMPERATURE,
            state["temperature"] - 0.08 * elapsed
        )


def _update_starting(state, elapsed):
    state["status"] = "STARTING"

    state["rotation_speed"] += 120 * elapsed
    state["pressure"] += 0.20 * elapsed
    state["power_consumption"] += 0.4 * elapsed
    state["temperature"] += 0.10 * elapsed

    if state["rotation_speed"] >= 1200:
        state["rotation_speed"] = 1200
        state["status"] = "RUNNING"


def _update_running(state, elapsed):
    load_factor = random.uniform(0.70, 0.95)

    target_rpm = 1200 + 600 * load_factor

    state["rotation_speed"] += (
        target_rpm - state["rotation_speed"]
    ) * 0.15 * elapsed

    state["rotation_speed"] += random.gauss(0, 5)

    heating_rate = (
        0.02
        + 0.07 * load_factor
    )

    state["temperature"] += (
        heating_rate * elapsed
        + random.gauss(0, 0.02)
    )

    target_pressure = (
        2.0
        + 0.0025 * state["rotation_speed"]
        + 0.02 * max(state["temperature"] - 60, 0)
    )

    state["pressure"] += (
        target_pressure - state["pressure"]
    ) * 0.20 * elapsed

    state["pressure"] += random.gauss(0, 0.02)

    target_power = (
        2.5
        + 0.006 * state["rotation_speed"]
        + 0.35 * state["pressure"]
    )

    state["power_consumption"] += (
        target_power - state["power_consumption"]
    ) * 0.20 * elapsed

    state["power_consumption"] += random.gauss(0, 0.05)

    state["runtime_seconds"] += elapsed

    if state["temperature"] >= COOLING_THRESHOLD:
        state["status"] = "COOLING"


def _update_cooling(state, elapsed):
    state["rotation_speed"] += (
        700 - state["rotation_speed"]
    ) * 0.25 * elapsed

    state["temperature"] -= 0.15 * elapsed

    state["pressure"] += (
        3.0 - state["pressure"]
    ) * 0.20 * elapsed

    state["power_consumption"] += (
        5.0 - state["power_consumption"]
    ) * 0.20 * elapsed

    state["runtime_seconds"] += elapsed

    if state["temperature"] <= COOLING_TARGET:
        state["status"] = "RUNNING"


def _update_error(state, elapsed):
    state["status"] = "ERROR"

    state["rotation_speed"] = max(
        0.0,
        state["rotation_speed"] - 400 * elapsed
    )

    state["power_consumption"] = max(
        0.0,
        state["power_consumption"] - 1.5 * elapsed
    )

    state["pressure"] = max(
        0.0,
        state["pressure"] - 0.3 * elapsed
    )


def generate_machine_a(machine_id: str = "A-001") -> dict:
    if machine_id not in machine_states:
        machine_states[machine_id] = _get_initial_state()

    state = machine_states[machine_id]

    now = datetime.now(UTC)

    elapsed = max(
        0.0,
        (now - state["last_update"]).total_seconds()
    )

    state["last_update"] = now

    if (
        state["temperature"] >= ERROR_TEMPERATURE
        or state["pressure"] >= ERROR_PRESSURE
    ):
        state["status"] = "ERROR"

    if not _is_operating_time(now):
        _update_off(state, elapsed)

    elif state["status"] == "OFF":
        _update_starting(state, elapsed)

    elif state["status"] == "STARTING":
        _update_starting(state, elapsed)

    elif state["status"] == "RUNNING":
        _update_running(state, elapsed)

    elif state["status"] == "COOLING":
        _update_cooling(state, elapsed)

    elif state["status"] == "ERROR":
        _update_error(state, elapsed)

    return {
        "timestamp": now.isoformat(),
        "machine_id": machine_id,
        "status": state["status"],
        "temperature": round(state["temperature"], 2),
        "pressure": round(state["pressure"], 2),
        "rotation_speed": round(state["rotation_speed"], 2),
        "power_consumption": round(
            state["power_consumption"], 2
        ),
        "runtime_seconds": round(
            state["runtime_seconds"], 2
        ),
    }