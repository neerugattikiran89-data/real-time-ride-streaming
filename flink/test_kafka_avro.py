import json
import sys

from confluent_kafka import Consumer

sys.path.insert(0, "flink")

from avro_decoder import decode_confluent_avro

with open("schemas/ride_request.avsc", "r") as f:
    schema = json.load(f)

consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "avro-decoder-test",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["ride_requests"])

print("Waiting for a Kafka ride event...")

try:
    msg = consumer.poll(10)

    if msg is None:
        print("No message received within 10 seconds.")

    elif msg.error():
        print("Kafka error:", msg.error())

    else:
        decoded_event = decode_confluent_avro(
            msg.value(),
            schema
        )

        print("\nSuccessfully decoded Avro event:")
        print(json.dumps(decoded_event, indent=2))

finally:
    consumer.close()