import json
from dataclasses import asdict
from kafka import KafkaProducer


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send_event(topic, event):
    producer.send(topic, asdict(event))
    producer.flush()