import random
from datetime import UTC, datetime


def generate_machine_c(machine_id: str = "C-001") -> str:
    timestamp = datetime.now(UTC).isoformat()
    temperature = round(random.uniform(70, 100), 2)
    status = random.choice([
        "RUNNING",
        "STOPPED",
        "ERROR"
    ])

    return (
        f"{timestamp}|"
        f"{machine_id}|"
        f"{temperature}|"
        f"{status}"
    )