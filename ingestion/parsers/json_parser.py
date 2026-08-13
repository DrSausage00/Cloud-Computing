from schema.unified_schema import MachineEvent


def parse_machine_b(data):
    return MachineEvent(
        timestamp=data["ts"],
        machine_id=data["id"],
        machine_type="B",
        temperature=data["temp"],
        vibration=data["vibration"]
    )