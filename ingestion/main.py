from dataclasses import asdict
import json
import time

from simulators.machine_a import generate_machine_a
from simulators.machine_b import generate_machine_b
from simulators.machine_c import generate_machine_c

from parsers.csv_parser import parse_machine_a
from parsers.json_parser import parse_machine_b
from parsers.pipe_parser import parse_machine_c

from producer.kafka_producer import send_event


MACHINE_A_COUNT = 10
MACHINE_B_COUNT = 5
MACHINE_C_COUNT = 3


def _append_machine_events(events, family_count, generator, parser):
    for i in range(family_count):
        events.append(parser(generator()))


def main():

    while True:
        events = []

        for i in range(max(MACHINE_A_COUNT, MACHINE_B_COUNT, MACHINE_C_COUNT)):
            if i < MACHINE_A_COUNT:
                events.append(parse_machine_a(generate_machine_a()))
            if i < MACHINE_B_COUNT:
                events.append(parse_machine_b(generate_machine_b()))
            if i < MACHINE_C_COUNT:
                events.append(parse_machine_c(generate_machine_c()))

        for event in events:
            print(json.dumps(asdict(event), indent=2))
            send_event("machine-events", event)

        time.sleep(2)


if __name__ == "__main__":
    main()
