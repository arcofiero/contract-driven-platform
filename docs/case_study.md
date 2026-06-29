# Contract-Driven Data Platform: Engineering Case Study

**Author:** Archit Raj  
**Duration:** 10-day build  
**Stack:** Confluent Kafka · Avro · Spark (micro-batch) · Delta Lake · dbt · Soda Core · Airflow · Superset · Terraform

---

## The Problem

Most data pipelines fail silently.

A producer renames a field. A downstream model starts returning NULL for a column it previously aggregated. The dashboard shows zeros. No alert fires. Three days later, someone asks why the revenue chart flatlined — and the debugging begins.

The root cause is almost always the same: data contracts don't exist. There's an implicit agreement between the team writing events and the team reading them, but it's never codified, never enforced, and never versioned. When it breaks, nobody knows when or where.

This project builds the opposite of that.

---

## What Was Built

A medallion data platform where **every byte entering the system must pass a schema contract before it is stored**, every rejected event is quarantined with full context, and data quality is validated at every layer transition.

```
Kafka Topics (4 sources)
  ↓  Avro + Schema Registry (contract at write time)
  ↓
Kafka Ingest Consumer
  ↓ [valid]    Delta Lake Bronze   ← raw + metadata
  ↓ [invalid]  DLQ topic + Delta Bronze/DLQ
  ↓
dbt Silver   ← typed, cleaned, normalized
  ↓
dbt Gold     ← business aggregates
  ↓
Soda Core    ← automated contract checks (12 check files, 3 layers)
  ↓
Airflow      ← orchestration + alerting
  ↓
Superset     ← business dashboard
```

---

## Day-by-Day Build

### Day 1 — Project Scaffold
Established the repo layout, conda environment, and `.env.example` pattern. All secrets are loaded at runtime via `python-dotenv`; no credentials ever touch git.

### Day 2 — Kafka Producers
Four Python generators publish to Confluent Cloud topics:

| Producer | Rate | Topic |
|---|---|---|
| Orders | ~1 msg/sec | `orders` |
| Clickstream | ~5 msg/sec | `clickstream` |
| Payments | ~0.5 msg/sec | `payments` |
| Weather | Every 10 min | `weather` |

Each producer intentionally injects ~5% malformed events (null required fields, out-of-range values, wrong types) to exercise the DLQ path end-to-end.

### Day 3 — Avro Schemas + Schema Registry
Every topic is backed by an Avro schema registered in Confluent Schema Registry. Producers use `AvroSerializer`; the consumer uses `AvroDeserializer`. A schema change that breaks the contract fails at the producer before a single event reaches the broker.

Key schemas:
- `order_event.avsc` — nested `items[]` array and `shipping_address` struct
- `clickstream_event.avsc` — page URL, session, user agent
- `payment_event.avsc` — amount, method, provider, status
- `weather_event.avsc` — temperature, humidity, wind, city

### Day 4 — Kafka Ingest Consumer → Delta Lake Bronze
The core ingestion layer. A micro-batch consumer polls all four topics, deserializes Avro, validates required fields, and routes:

- **Valid events** → `BronzeWriter` buffer → Delta Lake `bronze/<topic>`
- **Invalid events** → `DLQHandler` → DLQ Kafka topic + `bronze/dlq` Delta table

Every record receives ingestion metadata (`_ingested_at`, `_kafka_partition`, `_kafka_offset`, `_schema_version`, `_is_valid`) before being written. Offsets are committed only after a successful Delta write — at-least-once delivery with auditable lineage.

A field-name normalization layer (`_normalize_record`) translates Avro field names to Bronze schema column names, making the mapping explicit and testable.

**Bronze write volumes (Day 9 run):**

| Table | Rows |
|---|---|
| bronze/orders | 17,000+ |
| bronze/clickstream | 56,000+ |
| bronze/payments | 16,000+ |
| bronze/weather | 3,000+ |
| bronze/dlq | 15,000+ |

### Day 5 — dbt Silver + Gold
**Silver** cleans and normalizes Bronze:
- Type casting (strings → doubles, epoch ms → TIMESTAMP)
- Case normalization (`UPPER(TRIM(currency))`, `LOWER(TRIM(status))`)
- NULL filtering and range validation
- `silver_orders` uses `LATERAL VIEW EXPLODE(items)` to unpack the nested array into one row per line item
- `silver_dlq_audit` adds a human-readable `error_category` column

**Gold** produces business-ready aggregates:
- `gold_daily_order_revenue` — daily revenue, order count, avg unit price by currency
- `gold_payment_success_rate` — success/failure rates by payment method per day
- `gold_pipeline_health` — DLQ violation counts by topic and error type

All models write to Delta Lake with `file_format='delta'`.

### Day 6 — Soda Core Contract Checks
12 YAML check files validate data at all three layers using Soda Core (soda-core-spark-df).

**Bronze checks (per topic):** row count > 0, required fields not null, DLQ rate < 10%, numeric sanity, freshness < 30 minutes.

**Silver checks (per topic):** no nulls on key business fields, value ranges (temperature, humidity, amount), referential integrity.

**Gold checks:** min/max bounds on aggregated metrics, no future dates, pipeline health row count.

All 12 check files pass green. The runner (`run_soda_checks.py`) exits with code 0 on full pass, code 1 on any failure — making it directly usable in CI.

### Day 7 — Airflow Orchestration
Two DAGs:

- **`pipeline_orchestrator`** — sequences: producers → Kafka ingest consumer → dbt → Soda checks → Superset refresh
- **`contract_validation_dag`** — runs Soda checks on a schedule independently, alerts on failure

Tasks are templated with `BashOperator` and `PythonOperator`. The DAG dependency graph enforces that dbt Silver never runs before Bronze is populated, and Soda checks never run before dbt completes.

### Day 8 — Superset Dashboard
Three dashboard panels visible in a single scrolling view:

1. **Daily Gross Revenue by Currency** — stacked bar, 14-day window
2. **Daily Order Count** — line chart
3. **Payment Success Rate by Method** — grouped bar
4. **Contract Violations by Topic and Error Type** — heatmap
5. **DLQ Error Type Breakdown** — pie chart

Screenshots committed at `observability/dashboards/screenshots/`. Full-page capture via Playwright headless Chromium (1920×9800px — all 3 dashboard rows).

### Day 9 — Local-First End-to-End Run
Made the entire pipeline runnable locally without AWS:

- `USE_S3=false` — all Delta paths resolve to `/tmp/contract-driven-platform/delta/`
- AWS credentials not required when S3 is off
- Delta JARs loaded into dbt via `~/.dbt/profiles.yml` `spark.jars`
- PySpark 3.5.5 + delta-spark 3.3.2 for Soda compatibility (Soda requires PySpark 3.x)

**Final local run result: 12/12 Soda checks green, 8/8 dbt models pass.**

### Day 10 — Case Study + Publish
Documentation pass, `sync_gold_to_superset.py` to connect real pipeline data to Superset, and this case study.

---

## Key Engineering Decisions

### Why Avro over JSON?
Avro enforces schema at write time against Schema Registry. A producer trying to send an event with a missing required field will fail before the event hits the broker. JSON has no such guarantee — the message is accepted; the problem surfaces hours later in a downstream model.

### Why Delta Lake over plain Parquet?
Three reasons: ACID transactions (no partial writes visible to readers), time travel (every Bronze state is queryable), and schema enforcement at the storage layer. Delta's transaction log also makes it trivial to audit what was written and when.

### Why a Dead Letter Queue instead of dropping bad events?
Silent data loss is strictly worse than visible failure. Every rejected event carries the original bytes, the rejection reason, the error type, partition, and offset. This means:
- You can replay and re-ingest after fixing the upstream schema
- You can measure the exact contract violation rate per topic
- You have an audit trail for compliance

### Why field-name normalization in the consumer rather than dbt?
The `_normalize_record()` function in `kafka_consumer.py` maps Avro field names (e.g. `temperature_c`, `payment_method`) to Bronze schema names (`temperature`, `method`) at ingest time. Doing it here keeps Bronze schemas stable — dbt Silver models can be written assuming canonical column names without worrying about what the upstream Avro revision called the field.

### Why at-least-once over exactly-once?
Kafka offset commits happen only after a successful Delta write. If the process crashes mid-flush, the same messages are re-consumed and re-written. Delta's ACID guarantees mean a duplicate write is idempotent at the file level. The tradeoff: Silver deduplication (using `DISTINCT` or surrogate key dedup) handles the rare duplicate. This is simpler to operate than Kafka transactions with no meaningful correctness cost.

---

## Challenges and What They Taught

**PySpark schema mismatch on nested types.** The orders Bronze schema originally used flat `product_id`, `quantity`, `unit_price` columns. The actual Avro had a nested `items: [{product_id, name, unit_price, quantity, subtotal}]` array and a `shipping_address` struct. PySpark silently dropped mismatched rows instead of raising an error. Fix: `ArrayType(ORDER_ITEM_SCHEMA)` in the schema definition and `LATERAL VIEW EXPLODE` in dbt Silver.

**Timezone-aware datetimes rejected by PySpark TimestampType.** `datetime.now(timezone.utc).isoformat()` produces a tz-aware string. PySpark 3.5 `TimestampType` rejects it with a cryptic serialization error. Fix: strip tzinfo before storing — `datetime.now(timezone.utc).replace(tzinfo=None)`.

**dbt ClassNotFoundException for DeltaCatalog.** dbt-spark creates its own SparkSession and doesn't load Delta JARs automatically. Fix: add `spark.jars` in `~/.dbt/profiles.yml` pointing to the exact JAR paths from the ivy2 cache. This is non-obvious because the error message points to a class, not a configuration key.

**Soda API changes between versions.** `scan.has_check_failures()` was renamed `scan.has_check_fails()`; `scan.get_checks()` was replaced by `scan.get_checks_fail_text()`. Both fail silently with AttributeError. Lesson: pin Soda Core versions and read changelogs before upgrading.

---

## Results

| Metric | Value |
|---|---|
| End-to-end latency (Kafka → Gold) | < 90 seconds |
| Contract violation detection | < 3 seconds |
| DLQ capture rate | 100% |
| Soda checks | 12/12 passing |
| dbt models | 8/8 passing |
| Bronze row volume (single run) | 90,000+ events |
| DLQ quarantine rate | ~5% (intentional) |
| Local run: AWS required? | No |

---

## What I Would Do Differently in Production

1. **Exactly-once delivery** via Kafka transactions + Delta `MERGE INTO` with dedup keys, rather than at-least-once + downstream dedup.

2. **Schema evolution strategy** — document what changes are backward-compatible (adding optional fields) vs. breaking (removing fields, changing types) and enforce via CI checks against Schema Registry.

3. **Separate Spark clusters for ingestion and transformation** — the Kafka ingest consumer and dbt share a JVM in this build; in production they'd be separate compute with separate scaling policies.

4. **Alerting integration** — Soda check failures currently exit with code 1. In production, pipe those to PagerDuty or Slack via Soda's built-in notification hooks.

5. **Data lineage UI** — OpenLineage is in the stack; wire it to Marquez for a visual graph of dataset-to-dataset dependencies. Invaluable when debugging which upstream model caused a downstream failure.

---

## Repository

[github.com/arcofiero/contract-driven-platform](https://github.com/arcofiero/contract-driven-platform)

---

*Built in 10 days. Every layer is real — no mocks, no stubs, no "coming soon."*
