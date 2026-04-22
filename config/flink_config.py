"""
config/flink_config.py
-----------------------
Central config for Day 4: Flink + Confluent Kafka + Delta Lake on S3.
All credentials loaded from .env via python-dotenv. No hardcoded secrets.
"""

import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)


def _require(key: str) -> str:
    val = os.getenv(key)
    if not val:
        raise EnvironmentError(
            f"Required environment variable '{key}' is missing. Check your .env file."
        )
    return val


def _optional(key: str, default: str = "") -> str:
    return os.getenv(key, default)


# Confluent Kafka
KAFKA_CONFIG = {
    "bootstrap.servers":    _require("CONFLUENT_BOOTSTRAP_SERVERS"),
    "security.protocol":    "SASL_SSL",
    "sasl.mechanisms":      "PLAIN",
    "sasl.username":        _require("CONFLUENT_API_KEY"),
    "sasl.password":        _require("CONFLUENT_API_SECRET"),
    "group.id":             _optional("FLINK_CONSUMER_GROUP", "flink-bronze-consumer"),
    "auto.offset.reset":    _optional("KAFKA_AUTO_OFFSET_RESET", "earliest"),
    "enable.auto.commit":   False,
    "session.timeout.ms":   30000,
    "max.poll.interval.ms": 300000,
}

# Confluent Schema Registry
SCHEMA_REGISTRY_CONFIG = {
    "url": _require("CONFLUENT_SCHEMA_REGISTRY_URL"),
    "basic.auth.user.info": (
        f"{_require('CONFLUENT_SCHEMA_REGISTRY_API_KEY')}:{_require('CONFLUENT_SCHEMA_REGISTRY_API_SECRET')}"
    ),
}

# Kafka Topics
TOPIC_ORDERS      = _optional("TOPIC_ORDERS",      "orders")
TOPIC_CLICKSTREAM = _optional("TOPIC_CLICKSTREAM", "clickstream")
TOPIC_PAYMENTS    = _optional("TOPIC_PAYMENTS",    "payments")
TOPIC_WEATHER     = _optional("TOPIC_WEATHER",     "weather")
TOPIC_DLQ         = _optional("TOPIC_DLQ",         "dead-letter-queue")

ALL_SOURCE_TOPICS = [TOPIC_ORDERS, TOPIC_CLICKSTREAM, TOPIC_PAYMENTS, TOPIC_WEATHER]

# AWS S3 / Delta Lake
AWS_ACCESS_KEY_ID     = _require("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = _require("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = _optional("AWS_REGION", "us-east-1")
S3_BUCKET             = _require("S3_BUCKET")
DELTA_BASE_PATH       = f"s3a://{S3_BUCKET}/delta"

# PySpark / Delta
SPARK_APP_NAME   = _optional("SPARK_APP_NAME", "contract-driven-bronze-writer")
SPARK_MASTER     = _optional("SPARK_MASTER", "local[*]")
DELTA_WRITE_MODE = _optional("DELTA_WRITE_MODE", "append")
CHECKPOINT_BASE  = f"s3a://{S3_BUCKET}/checkpoints"

# Consumer tuning
BATCH_MAX_RECORDS = int(_optional("BATCH_MAX_RECORDS", "500"))
BATCH_TIMEOUT_SEC = float(_optional("BATCH_TIMEOUT_SEC", "10.0"))
POLL_TIMEOUT_SEC  = float(_optional("POLL_TIMEOUT_SEC", "1.0"))
MAX_RETRIES       = int(_optional("MAX_RETRIES", "3"))
RETRY_BACKOFF_SEC = float(_optional("RETRY_BACKOFF_SEC", "2.0"))

LOG_LEVEL = _optional("LOG_LEVEL", "INFO")


def get_spark_conf() -> dict:
    return {
        "spark.app.name":                              SPARK_APP_NAME,
        "spark.master":                                SPARK_MASTER,
        "spark.sql.extensions":                        "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":             "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.hadoop.fs.s3a.impl":                    "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.access.key":              AWS_ACCESS_KEY_ID,
        "spark.hadoop.fs.s3a.secret.key":              AWS_SECRET_ACCESS_KEY,
        "spark.hadoop.fs.s3a.endpoint":                f"s3.{AWS_REGION}.amazonaws.com",
        "spark.hadoop.fs.s3a.path.style.access":       "false",
        "spark.hadoop.fs.s3a.connection.ssl.enabled":  "true",
        "spark.sql.shuffle.partitions":                "8",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
    }


def log_config_summary() -> None:
    logger.info("=== Flink Consumer Config Summary ===")
    logger.info(f"  Bootstrap servers : {KAFKA_CONFIG['bootstrap.servers']}")
    logger.info(f"  Consumer group    : {KAFKA_CONFIG['group.id']}")
    logger.info(f"  Source topics     : {ALL_SOURCE_TOPICS}")
    logger.info(f"  DLQ topic         : {TOPIC_DLQ}")
    logger.info(f"  S3 bucket         : {S3_BUCKET}")
    logger.info(f"  Delta base path   : {DELTA_BASE_PATH}")
    logger.info(f"  Batch max records : {BATCH_MAX_RECORDS}")
    logger.info(f"  Batch timeout     : {BATCH_TIMEOUT_SEC}s")
    logger.info("=====================================")
