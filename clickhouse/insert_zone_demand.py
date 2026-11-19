
import subprocess
from datetime import datetime

def insert_zone_demand(zone, demand):
    query = (
        "INSERT INTO ride_analytics.zone_demand_history "
        "(event_time, zone, demand) "
        f"VALUES (now(), '{zone}', {int(demand)})"
    )

    result = subprocess.run(
        [
            "docker", "exec", "ride_clickhouse",
            "clickhouse-client", "-q", query
        ],
        capture_output=True,
        text=True
    )

    if result.returncode == 0:
        print(f"Inserted: {zone} | Demand: {demand}")
    else:
        print("Insert failed:", result.stderr)


if __name__ == "__main__":
    insert_zone_demand("midtown", 10)
    insert_zone_demand("lower_manhattan", 5)
    insert_zone_demand("upper_manhattan", 8)
