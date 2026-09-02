from schema.unified_schema import MachineEvent


FIELD_MAPPING = {
    "temp": "temperature"
}

METADATA_FIELDS = {
    "ts",
    "id",
    "type"
}


def parse_machine_b(data: dict) -> MachineEvent:
    measurements = {}

    for key, value in data.items():
        if key in METADATA_FIELDS:
            continue

        normalized_key = FIELD_MAPPING.get(key, key)
        measurements[normalized_key] = value

    return MachineEvent(
        timestamp=data["ts"],
        machine_id=data["id"],
        machine_type="B",
        measurements=measurements
    )