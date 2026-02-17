import sys
from typing import Any

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext


def delivery_report(err: Any, msg: Any) -> None:
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def test_kafka() -> None:
    conf: dict[str, Any] = {
        "bootstrap.servers": "localhost:9092",
        "client.id": "debug-client",
    }
    schema_registry_conf: dict[str, str] = {"url": "http://localhost:8081"}

    schema_str = """
    {
        "type": "record",
        "name": "User",
        "fields": [
            {"name": "name", "type": "string"},
            {"name": "age", "type": "int"}
        ]
    }
    """

    try:
        # Check SR
        print("Checking Schema Registry...")
        sr = SchemaRegistryClient(schema_registry_conf)
        subjects = sr.get_subjects()
        print(f"Subjects: {subjects}")

        # Check metadata
        a = AdminClient(conf)
        md = a.list_topics(timeout=10)
        print(f"Cluster metadata: {len(md.topics)} topics")

        # Produce Avro message
        print("Preparing Avro Serializer...")
        avro_serializer = AvroSerializer(sr, schema_str)

        p = Producer(conf)
        topic = "debug_avro_test"

        user = {"name": "Bob", "age": 42}

        print("Serializing...")
        val = avro_serializer(user, SerializationContext(topic, MessageField.VALUE))

        print(f"Producing to {topic}...")
        p.produce(topic, value=val, callback=delivery_report)
        print("Flushing messages...")
        p.flush(10)
        print("Success")

    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    test_kafka()
