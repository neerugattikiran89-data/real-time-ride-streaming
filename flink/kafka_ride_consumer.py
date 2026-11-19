from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.common.watermark_strategy import WatermarkStrategy

# Create Flink execution environment
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

# Configure Kafka source
kafka_source = (
    KafkaSource.builder()
    .set_bootstrap_servers("kafka:9092")
    .set_topics("ride_requests")
    .set_group_id("flink-ride-consumer")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

# Create streaming data pipeline
stream = env.from_source(
    kafka_source,
    WatermarkStrategy.no_watermarks(),
    "Kafka Ride Requests Source"
)

# Print incoming events
stream.print()

# Execute job
env.execute("Kafka Ride Requests Streaming")