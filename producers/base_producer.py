"""Shared Kafka producer utilities."""
import os
import logging
from confluent_kafka import Producer

logger = logging.getLogger(__name__)


def build_producer() -> Producer:
    """Build a Confluent Kafka producer from environment variables."""
    bootstrap = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
    api_key = os.environ["KAFKA_API_KEY"]
    api_secret = os.environ["KAFKA_API_SECRET"]

    config = {
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
        "compression.type": "snappy",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 300,
        "enable.idempotence": True,
    }

    # Allow local plaintext override (docker-compose dev)
    if bootstrap in ("localhost:9092", "broker:29092"):
        config = {
            "bootstrap.servers": bootstrap,
            "compression.type": "snappy",
            "acks": "all",
        }

    return Producer(config)


def delivery_callback(err, msg):
    """Log per-message delivery report."""
    if err:
        logger.error("Delivery failed | topic=%s partition=%s error=%s",
                     msg.topic(), msg.partition(), err)
    else:
        logger.debug("Delivered | topic=%s partition=%s offset=%s",
                     msg.topic(), msg.partition(), msg.offset())


def safe_produce(producer: Producer, topic: str, value: bytes,
                 key: bytes | None = None) -> None:
    """Produce with backpressure handling."""
    try:
        producer.produce(topic, value=value, key=key, callback=delivery_callback)
        producer.poll(0)
    except BufferError:
        logger.warning("Local queue full — flushing before retry")
        producer.flush(timeout=10)
        producer.produce(topic, value=value, key=key, callback=delivery_callback)
