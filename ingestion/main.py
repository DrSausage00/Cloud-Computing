from simulators.machine_a import generate_machine_a
from simulators.machine_b import generate_machine_b
from simulators.machine_c import generate_machine_c

from parsers.csv_parser import parse_machine_a
from parsers.json_parser import parse_machine_b
from parsers.pipe_parser import parse_machine_c

from producer.kafka_producer import send_event

import time

while True:

    a = parse_machine_a(generate_machine_a())
    b = parse_machine_b(generate_machine_b())
    c = parse_machine_c(generate_machine_c())

    send_event("machine-events", a)
    send_event("machine-events", b)
    send_event("machine-events", c)

    time.sleep(2)