import random
from datetime import datetime, UTC


def generate_machine_c():
    return (
        f"{datetime.now(UTC).isoformat()}|"
        f"C-001|"
        f"{round(random.uniform(70,100),2)}|"
        f"{random.choice(['RUNNING','STOPPED','ERROR'])}"
    )