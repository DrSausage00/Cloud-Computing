from dataclasses import asdict
import json
import os
import time

from simulators.machine_a import generate_machine_a
from simulators.machine_b import generate_machine_b
from simulators.machine_c import generate_machine_c

from parsers.csv_parser import parse_machine_a
from parsers.json_parser import parse_machine_b
from parsers.pipe_parser import parse_machine_c

from producer.kafka_producer import flush_events, send_event


# Welche Maschinentypen dieser Prozess simuliert. Mehrere Ingestion-Pods koennen
# so denselben Container-Build teilen und sich die Typen aufteilen
# (z. B. MACHINE_TYPES=A in Pod 1, MACHINE_TYPES=B,C in Pod 2).
MACHINE_TYPES = [
    machine_type.strip().upper()
    for machine_type in os.getenv("MACHINE_TYPES", "A,B,C").split(",")
    if machine_type.strip()
]

INTERVAL_SECONDS = float(os.getenv("INGESTION_INTERVAL_SECONDS", "2"))

# Events pro Typ und Durchlauf.
MACHINE_A_COUNT = int(os.getenv("MACHINE_A_COUNT", "10"))
MACHINE_B_COUNT = int(os.getenv("MACHINE_B_COUNT", "5"))
MACHINE_C_COUNT = int(os.getenv("MACHINE_C_COUNT", "3"))

MACHINE_FAMILIES = {
    "A": (MACHINE_A_COUNT, generate_machine_a, parse_machine_a),
    "B": (MACHINE_B_COUNT, generate_machine_b, parse_machine_b),
    "C": (MACHINE_C_COUNT, generate_machine_c, parse_machine_c),
}


def _resolve_families():
    unknown = [t for t in MACHINE_TYPES if t not in MACHINE_FAMILIES]

    if unknown:
        raise ValueError(
            f"Unbekannte Maschinentypen in MACHINE_TYPES: {', '.join(unknown)}. "
            f"Erlaubt: {', '.join(MACHINE_FAMILIES)}."
        )

    if not MACHINE_TYPES:
        raise ValueError("MACHINE_TYPES ist leer - kein Maschinentyp zu simulieren.")

    return [MACHINE_FAMILIES[t] for t in MACHINE_TYPES]


def _append_machine_events(events, family_count, generator, parser):
    for i in range(family_count):
        events.append(parser(generator()))


def main():
    families = _resolve_families()

    print(
        f"Ingestion startet fuer Maschinentypen: {', '.join(MACHINE_TYPES)} "
        f"(Takt: {INTERVAL_SECONDS}s)"
    )

    while True:
        events = []

        for count, generator, parser in families:
            _append_machine_events(events, count, generator, parser)

        for event in events:
            print(json.dumps(asdict(event), indent=2))
            send_event("machine-events", event)

        # Einmal je Durchlauf statt nach jeder Nachricht, damit Kafka buendeln kann.
        flush_events()

        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
