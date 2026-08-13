import random
from datetime import datetime, UTC


def generate_machine_a():
    return {
        "timestamp": datetime.now(UTC).isoformat(),
        "machine_id": "A-001",
        "temperature": round(random.uniform(60, 90), 2),
        "pressure": round(random.uniform(2, 8), 2)
    }

print(generate_machine_a())