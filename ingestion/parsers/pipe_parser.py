from schema.unified_schema import MachineEvent


def parse_machine_c(raw):
    ts, machine_id, temperature, status = raw.split("|")

    return MachineEvent(
        timestamp=ts,
        machine_id=machine_id,
        machine_type="C",
        temperature=float(temperature),
        status=status
    )