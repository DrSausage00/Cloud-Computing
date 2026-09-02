from schema.unified_schema import MachineEvent


METADATA_FIELDS = {
    "timestamp",
    "machine_id",
    "machine_type"
}


def parse_machine_a(data: dict) -> MachineEvent:
    measurements = {
        key: value
        for key, value in data.items()
        if key not in METADATA_FIELDS
    }

    return MachineEvent(
        timestamp=data["timestamp"],
        machine_id=data["machine_id"],
        machine_type="A",
        measurements=measurements
    )