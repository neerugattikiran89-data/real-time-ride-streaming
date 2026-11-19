
import json
import redis

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.window import TumblingProcessingTimeWindows
from pyflink.datastream.functions import WindowFunction
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy
from pyflink.common import Types, Time


env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = (
    KafkaSource.builder()
    .set_bootstrap_servers("ride_kafka:9092")
    .set_topics("ride_requests_json")
    .set_group_id("ride-zone-redis-analytics")
    .set_value_only_deserializer(SimpleStringSchema())
    .build()
)

stream = env.from_source(
    source,
    WatermarkStrategy.no_watermarks(),
    "Ride Requests JSON Source"
)


def extract_zone(event):
    return json.loads(event)["pickup_zone"]


zones = stream.map(extract_zone, output_type=Types.STRING())


class ZoneWindowCount(WindowFunction):
    def apply(self, key, window, inputs):
        count = sum(1 for _ in inputs)
        yield (key, count)


zone_counts = (
    zones
    .key_by(lambda zone: zone, key_type=Types.STRING())
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .apply(
        ZoneWindowCount(),
        output_type=Types.TUPLE([Types.STRING(), Types.LONG()])
    )
)


def write_to_redis(value):
    zone, count = value

    client = redis.Redis(
        host="ride_redis",
        port=6379,
        decode_responses=True
    )

    try:
        key = f"zone:{zone}:demand"
        client.set(key, int(count))
        message = f"Redis updated: {key} = {count}"
        return message
    finally:
        client.close()


redis_updates = zone_counts.map(
    write_to_redis,
    output_type=Types.STRING()
)

redis_updates.print()

env.execute("Real-Time Ride Demand to Redis")
