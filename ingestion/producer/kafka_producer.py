import json
import os 

from dataclasses import asdict
from kafka import KafkaProducer
KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS", 
    "localhost:9092"
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: json.dumps(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)


def send_event(topic, event):
    producer.send(topic, key=event.machine_id, value=asdict(event))
    producer.flush()