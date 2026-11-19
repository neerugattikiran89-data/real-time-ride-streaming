
from confluent_kafka import Consumer, KafkaException
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Kafka consumer configuration
consumer = Consumer({
    "bootstrap.servers": "localhost:29092",
    "group.id": "ride-streaming-test-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
})

# Schema Registry
schema_registry = SchemaRegistryClient({
    "url": "http://localhost:8085"
})

# Avro deserializer
avro_deserializer = AvroDeserializer(schema_registry)

# Subscribe to both topics
consumer.subscribe([
    "ride_requests",
    "driver_locations"
])

print("Listening for ride streaming events...\n")

try:
    count = 0

    while count < 10:
        message = consumer.poll(5.0)

        if message is None:
            print("Waiting for messages...")
            continue

        if message.error():
            raise KafkaException(message.error())

        topic = message.topic()

        event = avro_deserializer(
            message.value(),
            SerializationContext(topic, MessageField.VALUE)
        )

        print(f"\nTopic: {topic}")
        print(f"Event: {event}")

        count += 1

    print(f"\nSuccessfully decoded {count} Avro messages.")

except KeyboardInterrupt:
    print("Consumer stopped.")

finally:
    consumer.close()
