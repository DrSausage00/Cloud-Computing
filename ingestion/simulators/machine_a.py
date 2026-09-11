import random
from datetime import UTC, datetime


machine_states = {}
def _get_initial_state():
    return {
        "temperature": 22.0,
        "pressure": 0.0,
        "rotation_speed": 0.0,
        "power_cosumption": 0.0,
        "runtime_seconds": 0,
        "status": "OFF"
    }



def generate_machine_a(machine_id: str = "A-001") -> dict:

    if machine_id not in machine_states:
        machine_states[machine_id] = _get_initial_state()

        state = machine_states[machine_id]

        current_hour = datetime.now

        operating_time = 6 <= current_hour < 18

        if not operating_time:
            state["status"] = "OFF"

            state["rotation_speed"] = max(
                0.0, 
                state["rotation_speed"] -250 
            )

            state["pressure"] = max(
                0.0,
                state["pressure"] - 0.5
            )

            state["power_consumption"] = max(
                0.0,
                state["power_consumption"] - 2.0
            )

            if state["temperature"] > 22.0:
                state["temperature"] -= random.uniform(0.2,0.6)

        else:
            if state["status"] == "OFF":
                state["status"] = "STARTING"

            if state["status"] == "STARTING":
                state["rotation_speed"] += random.uniform(80, 150)
                state["pressure"] += random.uniform(0.2, 0.4)
                state["power_consumption"] += random.uniform(0.5, 1.0)
                state["temperature"] += random.uniform(0.3, 0.8)

                if state["rotation_speed"] >= 1200:
                    state["status"] = "RUNNING"

            elif state["status"] == "RUNNING":
                load_factor = random.uniform(0.65, 0.95)

                target_rpm = 1200 +600 *load_factor
                state["rotation_speed"] += (
                    0.05
                    + 0.35 * load_factor
                    + random.uniform(-0.1,0.1)
                )

                target_pressure = (
                    3.5
                    + 0.002 * state["rotation_speed"]
                    + 0.03 * (state["rotation_speed"] - 60)
                )
                state["power_consumption"] += 2

                if state["temperature"] >= 92:
                    state["status"] = "COOLING"

            elif state["status"] == "COOLING":
                state["rotation_speed"] += (
                    700 - state["rotation_speed"]
                ) * 0.25
                state["temperatur"] -= random.uniform(0.5, 1.0)

                state["pressure"] += (
                    3.0 -state ["pressure"]    
                ) * 0.2

                state["runtime_seconds"] += 2

                if state["temperature"] <= 105:
                    state["status"] = "ERROR"

                if state ["pressure"] >= 9:
                    state["status"] = "ERROR"

                if state["status"] == "ERROR":
                    state["rotation_speed"] = max(
                        0.0,
                        state["rotation_speed"] - 400
                    )

                    state["power_consumption"] - max(
                        0.0,
                        state["power_consumption"] - 2.0
                    )
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "machine_id": machine_id,
        "status": state["status"],
        "temperature": round(state["temperature"], 2),
        "pressure": round(state["pressure"], 2),
        "rotation_speed": round(state["rotation_speed"], 2),
        "power_consumption": round(state["power_consumption"], 2),
        "runtime_seconds": state["runtime_seconds"]
    }