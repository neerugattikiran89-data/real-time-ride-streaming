
import json
import os
import sys

sys.path.insert(0, os.path.abspath("flink"))

from confluent_kafka import Consumer, Producer
from avro_decoder import decode_confluent_avro


with open("schemas/ride_request.avsc", "r") as f:
    ride_schema = json.load(f)

with open("schemas/driver_location.avsc", "r") as f:
    driver_schema = json.load(f)


consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "ride-avro-json-bridge",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": True
})

producer = Producer({
    "bootstrap.servers": "localhost:29092"
})

consumer.subscribe(["ride_requests", "driver_locations"])

print("Avro-to-JSON bridge started...")
print("Listening to ride_requests and driver_locations")


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka consumer error:", msg.error())
            continue

        try:
            if msg.topic() == "ride_requests":
                schema = ride_schema
                output_topic = "ride_requests_json"

            elif msg.topic() == "driver_locations":
                schema = driver_schema
                output_topic = "driver_locations_json"

            else:
                continue

            decoded_event = decode_confluent_avro(
                msg.value(),
                schema
            )

            json_payload = json.dumps(decoded_event)

            producer.produce(
                topic=output_topic,
                value=json_payload.encode("utf-8")
            )

            producer.poll(0)

            event_id = decoded_event.get("ride_id") or decoded_event.get("driver_id")
            print(f"Forwarded {msg.topic()} -> {output_topic}: {event_id}")

        except Exception as e:
            print("Event processing error:", e)

except KeyboardInterrupt:
    print("\nBridge stopped by user.")

finally:
    consumer.close()
    producer.flush()
