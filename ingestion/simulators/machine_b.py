import random
from datetime import datetime, UTC


def generate_machine_b():
    return {
        "ts": datetime.now(UTC).isoformat(),
        "id": "B-001",
        "temp": round(random.uniform(50, 80), 2),
        "vibration": round(random.uniform(0, 10), 2)
    }