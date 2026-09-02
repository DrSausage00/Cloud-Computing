from schema.unified_schema import MachineEvent


DEFAULT_OUTPUT_FIELDS = [
    "temperature",
    "status"
]


def parse_machine_c(
    raw_data: str,
    output_fields: list[str] | None = None
) -> MachineEvent:
    fields = output_fields or DEFAULT_OUTPUT_FIELDS
    values = raw_data.split("|")

    expected_value_count = 2 + len(fields)

    if len(values) != expected_value_count:
        raise ValueError(
            f"Ungültiger Pipe-Datensatz: "
            f"{expected_value_count} Werte erwartet, "
            f"{len(values)} erhalten."
        )

    timestamp = values[0]
    machine_id = values[1]
    measurement_values = values[2:]

    measurements = {
        field_name: convert_value(value)
        for field_name, value in zip(
            fields,
            measurement_values
        )
    }

    return MachineEvent(
        timestamp=timestamp,
        machine_id=machine_id,
        machine_type="C",
        measurements=measurements
    )


def convert_value(value: str):
    try:
        return float(value)
    except ValueError:
        return value