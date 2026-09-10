import json
import os

from dataclasses import asdict
from kafka import KafkaProducer


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:9092"
)

# Der Standard-Partitionierer hasht die machine_id. Bei nur drei verschiedenen
# Schlüsseln landen alle drei zufällig auf derselben Partition — die effektive
# Parallelität im Topic wäre damit 1 statt 3. Stattdessen bestimmt der Index in
# dieser Liste die Partition (siehe resolve_partition).
MACHINE_IDS = [
    "A-001",
    "B-001",
    "C-001"
]

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    linger_ms=50
)


def resolve_partition(topic, machine_id):
    """
    Ordnet bekannte Maschinen festen Partitionen zu.

    Unbekannte Maschinen liefern None zurück und werden
    vom Kafka-Standardpartitionierer behandelt.
    """

    if machine_id not in MACHINE_IDS:
        return None

    partitions = producer.partitions_for(topic)

    if not partitions:
        return None

    machine_index = MACHINE_IDS.index(machine_id)

    return sorted(partitions)[machine_index % len(partitions)]


def send_event(topic, event):
    producer.send(
        topic=topic,
        key=event.machine_id,
        value=asdict(event),
        partition=resolve_partition(topic, event.machine_id)
    )


def flush_events():
    """
    Überträgt alle gepufferten Nachrichten.
    Sollte einmal pro Simulationsdurchlauf aufgerufen werden.
    """

    producer.flush()
