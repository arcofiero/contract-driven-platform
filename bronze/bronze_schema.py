"""
delta/bronze_schema.py
----------------------
Delta Lake Bronze layer schema definitions for all 4 data streams.
Bronze = raw, unmodified events with ingestion metadata appended.
No business transformations happen here — that is Silver's job.
"""

from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, DoubleType, IntegerType,
    TimestampType, BooleanType,
)

# Shared ingestion metadata columns appended to every Bronze table
INGESTION_METADATA_FIELDS = [
    StructField("_ingested_at",      TimestampType(), nullable=False),
    StructField("_source_topic",     StringType(),    nullable=False),
    StructField("_kafka_partition",  IntegerType(),   nullable=False),
    StructField("_kafka_offset",     LongType(),      nullable=False),
    StructField("_schema_version",   StringType(),    nullable=True),
    StructField("_is_valid",         BooleanType(),   nullable=False),
]

# Orders Bronze
BRONZE_ORDERS_SCHEMA = StructType([
    StructField("order_id",     StringType(),  nullable=False),
    StructField("customer_id",  StringType(),  nullable=False),
    StructField("product_id",   StringType(),  nullable=False),
    StructField("quantity",     IntegerType(), nullable=True),
    StructField("unit_price",   DoubleType(),  nullable=True),
    StructField("total_amount", DoubleType(),  nullable=True),
    StructField("currency",     StringType(),  nullable=True),
    StructField("status",       StringType(),  nullable=True),
    StructField("region",       StringType(),  nullable=True),
    StructField("event_ts",     LongType(),    nullable=True),
    StructField("event_date",   StringType(),  nullable=False),
] + INGESTION_METADATA_FIELDS)

# Clickstream Bronze
BRONZE_CLICKSTREAM_SCHEMA = StructType([
    StructField("event_id",   StringType(), nullable=False),
    StructField("user_id",    StringType(), nullable=False),
    StructField("session_id", StringType(), nullable=True),
    StructField("page",       StringType(), nullable=True),
    StructField("action",     StringType(), nullable=True),
    StructField("element",    StringType(), nullable=True),
    StructField("referrer",   StringType(), nullable=True),
    StructField("user_agent", StringType(), nullable=True),
    StructField("ip_address", StringType(), nullable=True),
    StructField("country",    StringType(), nullable=True),
    StructField("event_ts",   LongType(),   nullable=True),
    StructField("event_date", StringType(), nullable=False),
] + INGESTION_METADATA_FIELDS)

# Payments Bronze
BRONZE_PAYMENTS_SCHEMA = StructType([
    StructField("payment_id",  StringType(),  nullable=False),
    StructField("order_id",    StringType(),  nullable=False),
    StructField("customer_id", StringType(),  nullable=False),
    StructField("amount",      DoubleType(),  nullable=True),
    StructField("currency",    StringType(),  nullable=True),
    StructField("method",      StringType(),  nullable=True),
    StructField("status",      StringType(),  nullable=True),
    StructField("provider",    StringType(),  nullable=True),
    StructField("region",      StringType(),  nullable=True),
    StructField("event_ts",    LongType(),    nullable=True),
    StructField("event_date",  StringType(),  nullable=False),
] + INGESTION_METADATA_FIELDS)

# Weather Bronze
BRONZE_WEATHER_SCHEMA = StructType([
    StructField("city",        StringType(),  nullable=False),
    StructField("country",     StringType(),  nullable=True),
    StructField("temperature", DoubleType(),  nullable=True),
    StructField("feels_like",  DoubleType(),  nullable=True),
    StructField("humidity",    IntegerType(), nullable=True),
    StructField("pressure",    IntegerType(), nullable=True),
    StructField("wind_speed",  DoubleType(),  nullable=True),
    StructField("description", StringType(),  nullable=True),
    StructField("icon",        StringType(),  nullable=True),
    StructField("event_ts",    LongType(),    nullable=True),
    StructField("event_date",  StringType(),  nullable=False),
] + INGESTION_METADATA_FIELDS)

# DLQ Bronze
BRONZE_DLQ_SCHEMA = StructType([
    StructField("raw_payload",   StringType(), nullable=True),
    StructField("error_message", StringType(), nullable=False),
    StructField("error_type",    StringType(), nullable=False),
    StructField("source_topic",  StringType(), nullable=False),
    StructField("event_date",    StringType(), nullable=False),
] + INGESTION_METADATA_FIELDS)

# Registry: topic key -> schema + Delta table path
BRONZE_TABLE_REGISTRY = {
    "orders": {
        "schema":         BRONZE_ORDERS_SCHEMA,
        "delta_path":     "bronze/orders",
        "partition_cols": ["event_date"],
    },
    "clickstream": {
        "schema":         BRONZE_CLICKSTREAM_SCHEMA,
        "delta_path":     "bronze/clickstream",
        "partition_cols": ["event_date"],
    },
    "payments": {
        "schema":         BRONZE_PAYMENTS_SCHEMA,
        "delta_path":     "bronze/payments",
        "partition_cols": ["event_date"],
    },
    "weather": {
        "schema":         BRONZE_WEATHER_SCHEMA,
        "delta_path":     "bronze/weather",
        "partition_cols": ["event_date"],
    },
    "dlq": {
        "schema":         BRONZE_DLQ_SCHEMA,
        "delta_path":     "bronze/dlq",
        "partition_cols": ["event_date", "source_topic"],
    },
}
