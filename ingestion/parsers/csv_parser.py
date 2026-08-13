from schema.unified_schema import MachineEvent


def parse_machine_a(data):
    return MachineEvent(
        timestamp=data["timestamp"],
        machine_id=data["machine_id"],
        machine_type="A",
        temperature=data["temperature"],
        pressure=data["pressure"]
    )