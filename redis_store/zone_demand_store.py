import redis



class ZoneDemandStore:

    def __init__(self, host="localhost", port=6379):
        self.client = redis.Redis(
            host=host,
            port=port,
            decode_responses=True
        )

    def update_zone_demand(self, zone, count):
        key = f"zone:{zone}:demand"
        self.client.set(key, count)

    def get_zone_demand(self, zone):
        key = f"zone:{zone}:demand"
        value = self.client.get(key)

        if value is None:
            return 0

        return int(value)

    def get_all_zone_demand(self):
        result = {}

        for key in self.client.scan_iter("zone:*:demand"):
            zone = key.split(":")[1]
            result[zone] = int(self.client.get(key))

        return result

    def close(self):
        self.client.close()