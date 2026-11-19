
import argparse
import json
import time
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField

from event_simulator import generate_event


# Project paths
BASE_DIR = Path(__file__).resolve().parent.parent
SCHEMA_DIR = BASE_DIR / "schemas"

# Kafka configuration
producer = Producer({
    "bootstrap.servers": "localhost:29092"
})

# Schema Registry configuration
schema_registry = SchemaRegistryClient({
    "url": "http://localhost:8085"
})

# Load Avro schemas
with open(SCHEMA_DIR / "ride_request.avsc", "r") as file:
    ride_schema = file.read()

with open(SCHEMA_DIR / "driver_location.avsc", "r") as file:
    driver_schema = file.read()

# Create Avro serializers
ride_serializer = AvroSerializer(
    schema_registry,
    ride_schema,
    conf={"auto.register.schemas": False}
)

driver_serializer = AvroSerializer(
    schema_registry,
    driver_schema,
    conf={"auto.register.schemas": False}
)

delivered = 0
failed = 0


def delivery_report(err, message):
    global delivered, failed

    if err is not None:
        failed += 1
        print(f"Delivery failed: {err}")
    else:
        delivered += 1


def run_producer(events_per_second, duration_seconds):
    interval = 1.0 / events_per_second
    start_time = time.monotonic()
    generated = 0

    print("Starting Kafka producer...")

    try:
        while time.monotonic() - start_time < duration_seconds:
            event = generate_event()

            if event["event_type"] == "ride_requested":
                topic = "ride_requests"
                serializer = ride_serializer
                key = event["ride_id"]
            else:
                topic = "driver_locations"
                serializer = driver_serializer
                key = event["driver_id"]

            value = serializer(
                event,
                SerializationContext(topic, MessageField.VALUE)
            )

            producer.produce(
                topic=topic,
                key=key,
                value=value,
                on_delivery=delivery_report
            )

            producer.poll(0)
            generated += 1

            if generated % 100 == 0:
                print(f"Generated {generated} events")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("Producer stopped by user.")

    finally:
        remaining = producer.flush(30)

        print("\nProducer summary")
        print(f"Generated events: {generated}")
        print(f"Successfully delivered: {delivered}")
        print(f"Failed deliveries: {failed}")
        print(f"Messages remaining: {remaining}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--events-per-second",
        type=float,
        default=5
    )

    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=30
    )

    args = parser.parse_args()

    if args.events_per_second <= 0 or args.duration_seconds < 0:
        parser.error("Rate must be positive and duration cannot be negative.")

    run_producer(
        args.events_per_second,
        args.duration_seconds
    )
