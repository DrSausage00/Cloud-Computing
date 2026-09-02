from dataclasses import asdict
import json
import time

from simulators.machine_a import generate_machine_a
from simulators.machine_b import generate_machine_b
from simulators.machine_c import generate_machine_c

from parsers.csv_parser import parse_machine_a
from parsers.json_parser import parse_machine_b
from parsers.pipe_parser import parse_machine_c


MACHINE_A_COUNT = 10
MACHINE_B_COUNT = 5
MACHINE_C_COUNT = 3


def main():

    while True:

        events = []

        for _ in range(MACHINE_A_COUNT):
            events.append(
                parse_machine_a(
                    generate_machine_a()
                )
            )

        for _ in range(MACHINE_B_COUNT):
            events.append(
                parse_machine_b(
                    generate_machine_b()
                )
            )

        for _ in range(MACHINE_C_COUNT):
            events.append(
                parse_machine_c(
                    generate_machine_c()
                )
            )

        for event in events:
            print(json.dumps(asdict(event), indent=2))

        time.sleep(2)


if __name__ == "__main__":
    main()