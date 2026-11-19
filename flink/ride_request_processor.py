import json

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.datastream.connectors.kafka import KafkaSource

from raw_bytes_deserializer import RawBytesDeserializer
from avro_decoder import decode_confluent_avro


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

ride_schema = {
    "type": "record",
    "name": "RideRequest",
    "namespace": "com.ride.streaming",
    "fields": [
        {"name": "event_id", "type": "string"},
        {"name": "event_type", "type": "string"},
        {"name": "event_time", "type": "string"},
        {"name": "ride_id", "type": "string"},
        {"name": "rider_id", "type": "string"},
        {"name": "pickup_latitude", "type": "double"},
        {"name": "pickup_longitude", "type": "double"},
        {"name": "pickup_zone", "type": "string"},
        {"name": "destination_zone", "type": "string"}
    ]
}

kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("ride_requests")
    .set_group_id("flink-ride-avro-processor")
    .set_value_only_deserializer(RawBytesDeserializer())
    .build()
)

stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Ride Requests Kafka Source"
)

decoded_stream = stream.map(
    lambda message: decode_confluent_avro(message, ride_schema)
)

decoded_stream.map(
    lambda event: json.dumps(event, default=str)
).print()

env.execute("Structured Ride Request Streaming")