
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

event_type = Types.TUPLE([
    Types.STRING(),
    Types.LONG(),
    Types.LONG()
])


def create_source(topic, group_id):
    return (
        KafkaSource.builder()
        .set_bootstrap_servers("ride_kafka:9092")
        .set_topics(topic)
        .set_group_id(group_id)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )


ride_source = create_source(
    "ride_requests_json",
    "ride-demand-surge-analytics"
)

driver_source = create_source(
    "driver_locations_json",
    "ride-driver-surge-analytics"
)

rides = env.from_source(
    ride_source,
    WatermarkStrategy.no_watermarks(),
    "Ride Requests"
)

drivers = env.from_source(
    driver_source,
    WatermarkStrategy.no_watermarks(),
    "Driver Locations"
)


def parse_ride(event):
    data = json.loads(event)
    return (data["pickup_zone"], 1, 0)


def parse_driver(event):
    data = json.loads(event)

    available = 1 if data["is_available"] else 0

    return (data["zone_id"], 0, available)


ride_events = rides.map(
    parse_ride,
    output_type=event_type
)

driver_events = drivers.map(
    parse_driver,
    output_type=event_type
)

combined = ride_events.union(driver_events)


class DemandSupplyWindow(WindowFunction):
    def apply(self, zone, window, inputs):
        demand = 0
        supply = 0

        for event in inputs:
            demand += event[1]
            supply += event[2]

        yield (zone, demand, supply)


zone_stats = (
    combined
    .key_by(lambda event: event[0], key_type=Types.STRING())
    .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
    .apply(
        DemandSupplyWindow(),
        output_type=Types.TUPLE([
            Types.STRING(),
            Types.LONG(),
            Types.LONG()
        ])
    )
)


def calculate_surge(stats):
    zone, demand, supply = stats

    ratio = demand / max(supply, 1)

    if ratio <= 1.0:
        multiplier = 1.0
    elif ratio <= 1.5:
        multiplier = 1.2
    elif ratio <= 2.0:
        multiplier = 1.5
    elif ratio <= 3.0:
        multiplier = 1.8
    else:
        multiplier = 2.0

    client = redis.Redis(
        host="ride_redis",
        port=6379,
        decode_responses=True
    )

    try:
        pipe = client.pipeline()

        pipe.set(f"zone:{zone}:demand", demand)
        pipe.set(f"zone:{zone}:supply", supply)
        pipe.set(f"zone:{zone}:ratio", round(ratio, 2))
        pipe.set(f"zone:{zone}:surge_multiplier", multiplier)

        pipe.execute()
    finally:
        client.close()

    result = (
        f"Zone: {zone} | Demand: {demand} | "
        f"Available Drivers: {supply} | Ratio: {ratio:.2f} | "
        f"Surge: {multiplier:.1f}x"
    )

    return result


surge_results = zone_stats.map(
    calculate_surge,
    output_type=Types.STRING()
)

surge_results.print()

env.execute("Real-Time Demand Supply and Surge Pricing")
