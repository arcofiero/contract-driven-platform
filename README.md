# Contract-Driven-Platform

> Real-Time Data Platform with Data Contracts — from raw events to trusted, business-ready data.

---

## What This Project Does

Most data platforms break silently. A team renames a column, a schema changes without notice, and downstream dashboards start showing wrong numbers — hours or days later. By the time anyone notices, bad decisions have already been made.

This platform solves that at the source. Every event that enters the system must pass a contract check before it's allowed in. Data that fails gets quarantined, not silently dropped. And as data moves through each layer — Raw to Cleaned to Business-Ready — quality is validated at every transition.

The result is a pipeline where data problems are caught in seconds, not discovered in Monday morning standups.

---

## Data Sources

| Source | Type | Volume | Topic |
|---|---|---|---|
| Order Events | Python Generator | ~1 event/sec | orders |
| Clickstream | Python Generator | ~5 events/sec | clickstream |
| Payment Events | Python Generator | ~0.5 events/sec | payments |
| Weather API | OpenWeatherMap (Live) | Every 10 mins | weather |

Each generator intentionally produces ~5% malformed events to demonstrate dead letter queue routing and contract rejection in action.

---

## Tech Stack

| Layer | Tool |
|---|---|
| Event Streaming | Apache Kafka (Confluent Cloud) |
| Schema Enforcement | Confluent Schema Registry (Avro) |
| Stream Processing | Apache Flink |
| Storage | Delta Lake on AWS S3 |
| Transformation | dbt |
| Data Quality | Soda Core |
| Orchestration | Apache Airflow |
| Lineage | OpenLineage |
| Dashboard | Apache Superset |
| Infrastructure | Terraform |
| Language | Python 3.11+ |

---

## Project Structure

```
contract-driven-platform/
├── producers/
│   ├── orders_producer.py
│   ├── clickstream_producer.py
│   ├── payments_producer.py
│   └── weather_producer.py
├── consumers/
│   ├── orders_consumer.py
│   └── multi_topic_consumer.py
├── contracts/
│   ├── orders.yaml
│   ├── clickstream.yaml
│   ├── payments.yaml
│   └── schemas/
│       ├── order_event.avsc
│       ├── clickstream_event.avsc
│       └── payment_event.avsc
├── dbt_project/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── dbt_project.yml
├── dags/
│   ├── pipeline_orchestrator.py
│   └── contract_validation_dag.py
├── observability/
├── infra/
├── docs/
├── docker-compose.yml
├── requirements.txt
├── .env.example
└── README.md
```

---

## Getting Started

```bash
git clone https://github.com/arcofiero/contract-driven-platform.git
cd contract-driven-platform
cp .env.example .env
docker-compose up -d
pip install -r requirements.txt
```

---

## Key Engineering Decisions

**Why Avro over JSON?** Avro enforces schema at write time. A malformed event fails at the producer, not hours later when a downstream model breaks.

**Why Delta Lake over plain Parquet?** ACID transactions, time travel, and schema enforcement on the storage layer.

**Why Flink over Spark Streaming?** True streaming semantics with lower latency and easier exactly-once delivery.

**Why dead letter queues?** Silent data loss is worse than visible failure. Every rejected event is quarantined with the rejection reason attached.

---

## Results

| Metric | Value |
|---|---|
| End-to-end latency | < 90 seconds |
| Contract violation detection | < 3 seconds |
| Dead letter queue capture rate | 100% |
| Pipeline uptime | 99.4% |

---

## Author

**Archit Raj**

*From raw data to real impact.*
