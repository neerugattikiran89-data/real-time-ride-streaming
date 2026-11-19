# Real-Time Ride-Hailing Streaming Platform

A real-time data engineering project that simulates ride-hailing events and processes streaming data using Apache Kafka and Apache Flink. Live zone-demand metrics are stored in Redis, while ClickHouse is used for historical analytics and Grafana for visualization.

## Architecture

Python Event Simulator → Kafka → Avro → Schema Registry → Avro-to-JSON Bridge → Apache Flink → Redis / ClickHouse → Grafana

## Tech Stack

- **Python** – Event generation and streaming utilities
- **Apache Kafka** – Event streaming and message broker
- **Apache Avro** – Data serialization
- **Confluent Schema Registry** – Schema management
- **Apache Flink (PyFlink)** – Stream processing and zone-demand analytics
- **Redis** – Live zone-demand metrics
- **ClickHouse** – Historical analytics storage
- **Grafana** – Dashboard visualization
- **Docker & Docker Compose** – Containerized infrastructure

## Features

- Simulated ride requests and driver-location events
- Kafka-based event streaming
- Avro serialization and schema management
- Avro-to-JSON bridge for downstream stream processing
- Stream processing with Apache Flink
- Zone-level demand analytics
- Redis integration for live demand metrics
- ClickHouse analytics storage
- Grafana dashboard for visualizing demand

## Project Structure

```text
real-time-ride-streaming/
│
├── clickhouse/
│   └── insert_zone_demand.py
│
├── docs/
│
├── flink/
│   ├── Dockerfile
│   ├── avro_decoder.py
│   ├── avro_to_json_bridge.py
│   ├── ride_request_json_processor.py
│   ├── ride_zone_analytics.py
│   └── ...
│
├── grafana/
│
├── redis_store/
│   └── zone_demand_store.py
│
├── schemas/
│   ├── driver_location.avsc
│   └── ride_request.avsc
│
├── simulator/
│   ├── event_simulator.py
│   ├── kafka_consumer.py
│   └── kafka_producer.py
│
├── tests/
├── docker-compose.yml
└── README.md
```

## Data Flow

1. The Python simulator generates ride-hailing events such as ride requests and driver-location updates.
2. Events are published to Kafka topics.
3. Avro schemas define the event structure and serialization.
4. Schema Registry manages the schemas used by the producer.
5. An Avro-to-JSON bridge consumes serialized events and forwards decoded events to JSON Kafka topics.
6. Apache Flink consumes the streaming events and performs real-time processing and zone-demand calculations.
7. Redis stores zone-level demand metrics for live access.
8. ClickHouse stores historical zone-demand records for analytical queries.
9. Grafana visualizes the analytics through dashboards.

## Prerequisites

- Docker Desktop
- Python
- Git

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/neerugattikiran89-data/real-time-ride-streaming.git
cd real-time-ride-streaming
```

### 2. Start the infrastructure

```bash
docker compose up -d
```

### 3. Create a Python virtual environment

Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

### 4. Install Python dependencies

Install the dependencies required by the simulator and streaming utilities.

### 5. Start the event producer

```bash
python simulator/kafka_producer.py --events-per-second 5 --duration-seconds 30
```

The producer generates events and publishes them to Kafka using Avro serialization and Schema Registry.

## Services

| Service | Purpose |
|---|---|
| Kafka | Event streaming |
| Schema Registry | Avro schema management |
| Flink | Real-time event processing |
| Redis | Live zone-demand metrics |
| ClickHouse | Historical analytical storage |
| Grafana | Streaming analytics visualization |

## Analytics

The platform processes streaming ride events and calculates zone-level demand metrics. Redis is used for live demand data, while ClickHouse provides a table for historical zone-demand analytics.

Example analytical query:

```sql
SELECT
    zone,
    SUM(demand) AS total_demand
FROM ride_analytics.zone_demand_history
GROUP BY zone
ORDER BY total_demand DESC;
```

## Dashboard

Grafana is used to visualize ride-hailing analytics, including zone demand and historical data.

## Project Status

The core streaming infrastructure, event generation, Kafka integration, Avro serialization, Flink processing, Redis metrics, ClickHouse setup, and Grafana dashboard have been implemented.

The project is currently a portfolio implementation. The end-to-end pipeline and historical ingestion paths are not fully automated in every component, and the final producer verification was pending infrastructure availability.

## Future Improvements

- Fully automate live Flink-to-ClickHouse ingestion
- Improve monitoring and pipeline health checks
- Add data-quality validation
- Expand streaming analytics and dashboard metrics

## Author

**Kiran**

GitHub: [neerugattikiran89-data](https://github.com/neerugattikiran89-data)
