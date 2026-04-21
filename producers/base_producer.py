"""
Shared Kafka producer utilities.

Provides:
- build_producer()       — raw confluent-kafka Producer from env
- delivery_callback()    — per-message delivery logging
- safe_produce()         — produce pre-serialized bytes with backpressure + headers support
- safe_produce_avro()    — serialize via AvroSerializer then produce
"""
from __future__ import annotations

import logging
import os
from typing import Optional

from confluent_kafka import KafkaException, Producer
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)


def build_producer() -> Producer:
    bootstrap  = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    api_key    = os.environ["KAFKA_API_KEY"]
    api_secret = os.environ["KAFKA_API_SECRET"]

    conf: dict = {
        "bootstrap.servers":  bootstrap,
        "security.protocol":  "SASL_SSL",
        "sasl.mechanism":     "PLAIN",
        "sasl.username":      api_key,
        "sasl.password":      api_secret,
        "compression.type":   "snappy",
        "acks":               "all",
        "retries":            5,
        "retry.backoff.ms":   300,
        "enable.idempotence": True,
    }

    if bootstrap in ("localhost:9092", "broker:29092"):
        conf = {
            "bootstrap.servers": bootstrap,
            "compression.type":  "snappy",
            "acks":              "all",
        }

    logger.debug("Building Kafka producer | bootstrap=%s", bootstrap)
    return Producer(conf)


def delivery_callback(err, msg) -> None:
    if err:
        logger.error("Delivery failed | topic=%s partition=%s error=%s",
                     msg.topic(), msg.partition(), err)
    else:
        logger.debug("Delivered | topic=%s partition=%d offset=%d key=%s",
                     msg.topic(), msg.partition(), msg.offset(),
                     msg.key().decode("utf-8", errors="replace") if msg.key() else None)


def safe_produce(
    producer: Producer,
    topic: str,
    value: bytes,
    key: Optional[bytes] = None,
    headers: Optional[dict] = None,
) -> None:
    header_list = list(headers.items()) if headers else None
    try:
        producer.produce(topic, value=value, key=key, headers=header_list,
                         on_delivery=delivery_callback)
        producer.poll(0)
    except BufferError:
        logger.warning("Local queue full — flushing before retry | topic=%s", topic)
        producer.flush(timeout=10)
        producer.produce(topic, value=value, key=key, headers=header_list,
                         on_delivery=delivery_callback)
        producer.poll(0)
    except KafkaException as exc:
        logger.error("KafkaException producing to %s: %s", topic, exc)
        raise


def safe_produce_avro(
    producer: Producer,
    topic: str,
    serializer: AvroSerializer,
    record: dict,
    key: Optional[bytes] = None,
    headers: Optional[dict] = None,
) -> None:
    ctx = SerializationContext(topic, MessageField.VALUE)
    try:
        value_bytes = serializer(record, ctx)
    except Exception as exc:
        logger.error("Avro serialization failed | topic=%s fields=%s error=%s",
                     topic,
                     list(record.keys()) if isinstance(record, dict) else type(record).__name__,
                     exc)
        raise
    safe_produce(producer, topic, value_bytes, key=key, headers=headers)
