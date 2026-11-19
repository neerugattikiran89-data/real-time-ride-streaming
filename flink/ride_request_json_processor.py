from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy

# Create Flink streaming environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Kafka JSON source
source = (
    KafkaSource.builder()
    .set_bootstrap_servers("ride_kafka:9092")
    .set_topics("ride_requests_json")
    .set_group_id("flink-ride-json-processor")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# Read events from Kafka
stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Ride Requests JSON Source"
)

# Print incoming events
stream.print()

# Execute Flink job
env.execute("Ride Request JSON Streaming Processor")