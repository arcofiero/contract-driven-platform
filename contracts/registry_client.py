"""
Confluent Schema Registry client.

Registers Avro schemas and vends configured AvroSerializer / AvroDeserializer
instances ready for use in producers and consumers.

Usage:
    python -m contracts.registry_client        # register all schemas
"""
from __future__ import annotations

import json
import logging
import os
from pathlib import Path

from confluent_kafka.schema_registry import Schema, SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

SCHEMAS_DIR = Path(__file__).parent / "schemas"

SUBJECT_MAP: dict[str, tuple[str, str]] = {
    "orders":      ("order.avsc",       "orders-value"),
    "clickstream": ("clickstream.avsc", "clickstream-value"),
    "payments":    ("payment.avsc",     "payments-value"),
    "weather":     ("weather.avsc",     "weather-value"),
}


def _build_registry_client() -> SchemaRegistryClient:
    url        = os.environ["CONFLUENT_SCHEMA_REGISTRY_URL"]
    api_key    = os.environ["CONFLUENT_SCHEMA_REGISTRY_API_KEY"]
    api_secret = os.environ["CONFLUENT_SCHEMA_REGISTRY_API_SECRET"]
    conf = {
        "url": url,
        "basic.auth.user.info": f"{api_key}:{api_secret}",
    }
    logger.debug("Connecting to Schema Registry | url=%s", url)
    return SchemaRegistryClient(conf)


def _load_schema_str(filename: str) -> str:
    path = SCHEMAS_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Avro schema file not found: {path}")
    return path.read_text(encoding="utf-8")


def register_schema(client: SchemaRegistryClient, topic: str) -> int:
    filename, subject = SUBJECT_MAP[topic]
    schema_str = _load_schema_str(filename)
    schema = Schema(schema_str, schema_type="AVRO")
    try:
        schema_id = client.register_schema(subject, schema)
        logger.info("Schema registered | topic=%-12s subject=%-20s id=%d", topic, subject, schema_id)
        return schema_id
    except Exception as exc:
        logger.error("Schema registration failed | topic=%s error=%s", topic, exc)
        raise


def register_all_schemas() -> dict[str, int]:
    client = _build_registry_client()
    results: dict[str, int] = {}
    for topic in SUBJECT_MAP:
        results[topic] = register_schema(client, topic)
    logger.info("All schemas registered: %s", results)
    return results


def get_avro_serializer(topic: str) -> AvroSerializer:
    client = _build_registry_client()
    filename, _ = SUBJECT_MAP[topic]
    schema_str = _load_schema_str(filename)
    logger.debug("Building AvroSerializer | topic=%s schema=%s", topic, filename)
    return AvroSerializer(client, schema_str)


def get_avro_deserializer(topic: str) -> AvroDeserializer:
    client = _build_registry_client()
    filename, _ = SUBJECT_MAP[topic]
    schema_str = _load_schema_str(filename)
    logger.debug("Building AvroDeserializer | topic=%s schema=%s", topic, filename)
    return AvroDeserializer(client, schema_str)


def check_schema_compatibility(topic: str, new_schema_str: str) -> bool:
    client = _build_registry_client()
    _, subject = SUBJECT_MAP[topic]
    schema = Schema(new_schema_str, schema_type="AVRO")
    compatible = client.test_compatibility(subject, schema)
    logger.info("Compatibility check | topic=%s subject=%s compatible=%s", topic, subject, compatible)
    return compatible


def serialize_record(topic: str, record: dict) -> bytes:
    serializer = get_avro_serializer(topic)
    ctx = SerializationContext(topic, MessageField.VALUE)
    return serializer(record, ctx)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    schema_ids = register_all_schemas()
    print(json.dumps(schema_ids, indent=2))
