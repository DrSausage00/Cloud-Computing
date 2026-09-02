import random
from datetime import UTC, datetime


def generate_machine_b(machine_id: str = "B-001") -> dict:
    return {
        "ts": datetime.now(UTC).isoformat(),
        "id": machine_id,
        "temp": round(random.uniform(50, 80), 2),
        "vibration": round(random.uniform(0, 10), 2)
    }