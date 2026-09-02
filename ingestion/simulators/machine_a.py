import random
from datetime import UTC, datetime


def generate_machine_a(machine_id: str = "A-001") -> dict:
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "machine_id": machine_id,
        "temperature": round(random.uniform(60, 90), 2),
        "pressure": round(random.uniform(2, 8), 2),
        "rotation_speed": round(random.uniform(1000, 2000), 2),
        "power_consumption": round(random.uniform(5, 20), 2)
    }