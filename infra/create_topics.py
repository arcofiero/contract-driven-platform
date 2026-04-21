"""
Create all required Kafka topics + Dead Letter Queue on Confluent Cloud.

Run once during environment bootstrap:
    python -m infra.create_topics

Idempotent: topics that already exist are skipped, not treated as errors.
"""
from __future__ import annotations

import logging
import os
import sys

from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

_DLQ_NAME = os.getenv("TOPIC_DLQ", "dead-letter-queue")

TOPICS: list[dict] = [
    {"name": os.getenv("TOPIC_ORDERS",      "orders"),       "partitions": 3, "replication": 3},
    {"name": os.getenv("TOPIC_CLICKSTREAM", "clickstream"),  "partitions": 6, "replication": 3},
    {"name": os.getenv("TOPIC_PAYMENTS",    "payments"),     "partitions": 3, "replication": 3},
    {"name": os.getenv("TOPIC_WEATHER",     "weather"),      "partitions": 1, "replication": 3},
    {"name": _DLQ_NAME,                                      "partitions": 3, "replication": 3},
]

BASE_CONFIG: dict[str, str] = {
    "retention.ms":        str(7 * 24 * 60 * 60 * 1000),
    "cleanup.policy":      "delete",
    "compression.type":    "lz4",
    "min.insync.replicas": "2",
}

DLQ_OVERRIDES: dict[str, str] = {
    "retention.ms": str(30 * 24 * 60 * 60 * 1000),
}


def _build_admin_client() -> AdminClient:
    bootstrap  = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    api_key    = os.environ["KAFKA_API_KEY"]
    api_secret = os.environ["KAFKA_API_SECRET"]
    conf = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism":    "PLAIN",
        "sasl.username":     api_key,
        "sasl.password":     api_secret,
    }
    return AdminClient(conf)


def create_topics() -> bool:
    admin = _build_admin_client()
    new_topics: list[NewTopic] = []
    for spec in TOPICS:
        cfg = dict(BASE_CONFIG)
        if spec["name"] == _DLQ_NAME:
            cfg.update(DLQ_OVERRIDES)
        new_topics.append(NewTopic(
            spec["name"],
            num_partitions=spec["partitions"],
            replication_factor=spec["replication"],
            config=cfg,
        ))

    futures = admin.create_topics(new_topics)
    all_ok = True
    for topic_name, future in futures.items():
        try:
            future.result()
            logger.info("Created topic: %s", topic_name)
        except Exception as exc:
            err = str(exc)
            if "TOPIC_ALREADY_EXISTS" in err or "already exists" in err.lower():
                logger.info("Topic already exists (skipping): %s", topic_name)
            else:
                logger.error("Failed to create topic %s: %s", topic_name, exc)
                all_ok = False
    return all_ok


def list_topics() -> list[str]:
    admin = _build_admin_client()
    meta  = admin.list_topics(timeout=15)
    names = sorted(t for t in meta.topics if not t.startswith("_"))
    logger.info("Topics on cluster: %s", names)
    return names


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logger.info("Creating Kafka topics on Confluent Cloud ...")
    success = create_topics()
    existing = list_topics()
    print("\nTopics on cluster:")
    for t in existing:
        print(f"  {t}")
    sys.exit(0 if success else 1)
