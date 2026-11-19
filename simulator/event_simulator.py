
import json
import random
import time
import uuid
import argparse

from datetime import datetime, timezone


# NYC simulation zones
ZONES = {
    "lower_manhattan": {
        "lat_min": 40.7000,
        "lat_max": 40.7250,
        "lon_min": -74.0200,
        "lon_max": -73.9850
    },
    "midtown": {
        "lat_min": 40.7250,
        "lat_max": 40.7550,
        "lon_min": -74.0100,
        "lon_max": -73.9700
    },
    "upper_manhattan": {
        "lat_min": 40.7550,
        "lat_max": 40.8000,
        "lon_min": -73.9900,
        "lon_max": -73.9300
    }
}


def current_timestamp():
    return datetime.now(timezone.utc).isoformat(
        timespec="milliseconds"
    )


def generate_location(zone_id):
    zone = ZONES[zone_id]

    latitude = random.uniform(
        zone["lat_min"],
        zone["lat_max"]
    )

    longitude = random.uniform(
        zone["lon_min"],
        zone["lon_max"]
    )

    return round(latitude, 6), round(longitude, 6)


def generate_ride_request():

    zone_id = random.choice(list(ZONES.keys()))

    latitude, longitude = generate_location(zone_id)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "ride_requested",
        "event_time": current_timestamp(),
        "ride_id": str(uuid.uuid4()),
        "rider_id": f"rider_{random.randint(1000, 9999)}",
        "pickup_latitude": latitude,
        "pickup_longitude": longitude,
        "pickup_zone": zone_id,
        "destination_zone": random.choice(list(ZONES.keys()))
    }

    return event


def generate_driver_location():

    zone_id = random.choice(list(ZONES.keys()))

    latitude, longitude = generate_location(zone_id)

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": "driver_location",
        "event_time": current_timestamp(),
        "driver_id": f"driver_{random.randint(1000, 1999)}",
        "latitude": latitude,
        "longitude": longitude,
        "zone_id": zone_id,
        "is_available": random.choice([True, True, True, False])
    }

    return event


def generate_event():

    if random.random() < 0.30:
        return generate_ride_request()

    return generate_driver_location()


def run_simulator(events_per_second, duration_seconds):

    interval = 1 / events_per_second
    start_time = time.time()
    event_count = 0

    print("Starting Real-Time Ride Event Simulator...")
    print(f"Event rate: {events_per_second} events/second")
    print("Press CTRL+C to stop.\n")

    try:

        while True:

            if duration_seconds > 0:
                if time.time() - start_time >= duration_seconds:
                    break

            event = generate_event()

            print(json.dumps(event), flush=True)

            event_count += 1

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\nSimulator stopped by user.")

    finally:
        print(f"Total events generated: {event_count}")


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Real-Time Ride-Hailing Event Simulator"
    )

    parser.add_argument(
        "--events-per-second",
        type=int,
        default=5
    )

    parser.add_argument(
        "--duration-seconds",
        type=int,
        default=60
    )

    args = parser.parse_args()

    if args.events_per_second <= 0 or args.duration_seconds < 0:
        parser.error("Event rate must be positive and duration non-negative.")

    run_simulator(
        args.events_per_second,
        args.duration_seconds
    )
