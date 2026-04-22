"""
flink/flink_consumer.py
------------------------
Main entry point for Day 4.

Pipeline:
  Confluent Kafka (4 topics)
    -> Avro deserialization via Confluent Schema Registry
    -> Schema validation
    -> [valid]   -> BronzeWriter buffer -> Delta Lake Bronze (S3)
    -> [invalid] -> DLQHandler -> DLQ Kafka topic + bronze/dlq Delta table

Run:
  python flink/flink_consumer.py
"""

import logging
import os
import signal
import sys
from datetime import datetime, timezone
from typing import Optional

from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

from config.flink_config import (
    KAFKA_CONFIG,
    SCHEMA_REGISTRY_CONFIG,
    ALL_SOURCE_TOPICS,
    POLL_TIMEOUT_SEC,
    log_config_summary,
)
from flink.bronze_writer import BronzeWriter, build_spark_session
from flink.dlq_handler import DLQHandler, DLQErrorType

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("flink_consumer")

TOPIC_TO_KEY = {
    os.getenv("TOPIC_ORDERS",      "orders"):      "orders",
    os.getenv("TOPIC_CLICKSTREAM", "clickstream"): "clickstream",
    os.getenv("TOPIC_PAYMENTS",    "payments"):    "payments",
    os.getenv("TOPIC_WEATHER",     "weather"):     "weather",
}


def _extract_event_date(record: dict, topic_key: str) -> str:
    ts_ms = record.get("event_ts") or record.get("timestamp")
    if ts_ms:
        try:
            return datetime.fromtimestamp(int(ts_ms) / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        except (ValueError, OSError):
            pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _enrich_with_metadata(
    record: dict,
    msg: Message,
    topic_key: str,
    schema_version: Optional[str],
    is_valid: bool,
) -> dict:
    record["event_date"]       = _extract_event_date(record, topic_key)
    record["_ingested_at"]     = datetime.now(timezone.utc).isoformat()
    record["_source_topic"]    = msg.topic()
    record["_kafka_partition"] = msg.partition()
    record["_kafka_offset"]    = msg.offset()
    record["_schema_version"]  = schema_version
    record["_is_valid"]        = is_valid
    return record


def build_deserializers(
    sr_client: SchemaRegistryClient,
    topics: list[str],
) -> dict[str, AvroDeserializer]:
    deserializers: dict[str, AvroDeserializer] = {}
    for topic in topics:
        subject = f"{topic}-value"
        try:
            registered = sr_client.get_latest_version(subject)
            schema_str = registered.schema.schema_str
            deserializers[topic] = AvroDeserializer(
                schema_registry_client=sr_client,
                schema_str=schema_str,
                from_dict=lambda d, ctx: d,
            )
            logger.info(
                f"Loaded Avro schema for '{subject}' "
                f"(id={registered.schema_id}, version={registered.version})"
            )
        except Exception as e:
            logger.error(f"Could not load schema for '{subject}': {e}")
    return deserializers


class FlinkConsumer:
    """
    Micro-batch Kafka -> Delta Lake Bronze consumer.
    Offsets committed only after successful Delta write (at-least-once).
    """

    def __init__(self):
        self._running = False

        logger.info("Initializing SparkSession for Delta Lake writes...")
        self._spark  = build_spark_session()
        self._writer = BronzeWriter(self._spark)
        self._dlq    = DLQHandler()

        self._sr_client     = SchemaRegistryClient(SCHEMA_REGISTRY_CONFIG)
        self._deserializers = build_deserializers(self._sr_client, ALL_SOURCE_TOPICS)
        self._consumer      = Consumer(dict(KAFKA_CONFIG))

        self._metrics = {
            "messages_consumed": 0,
            "messages_valid":    0,
            "messages_dlq":      0,
            "flushes":           0,
            "start_time":        datetime.now(timezone.utc).isoformat(),
        }

    def start(self) -> None:
        self._running = True
        self._consumer.subscribe(ALL_SOURCE_TOPICS)
        logger.info(f"Subscribed to topics: {ALL_SOURCE_TOPICS}")
        log_config_summary()
        try:
            self._poll_loop()
        finally:
            self._shutdown()

    def stop(self, *_) -> None:
        logger.info("Shutdown signal received -- finishing current batch...")
        self._running = False

    def _poll_loop(self) -> None:
        logger.info("Consumer poll loop started.")
        while self._running:
            msg = self._consumer.poll(timeout=POLL_TIMEOUT_SEC)

            if msg is None:
                if self._writer.should_flush():
                    self._do_flush()
                continue

            if msg.error():
                self._handle_kafka_error(msg)
                continue

            self._metrics["messages_consumed"] += 1
            self._process_message(msg)

            if self._writer.should_flush():
                self._do_flush()

    def _process_message(self, msg: Message) -> None:
        topic        = msg.topic()
        topic_key    = TOPIC_TO_KEY.get(topic, topic)
        deserializer = self._deserializers.get(topic)
        schema_version: Optional[str] = None

        try:
            if deserializer is None:
                raise ValueError(f"No Avro deserializer for topic '{topic}'")
            ctx    = SerializationContext(topic, MessageField.VALUE)
            record = deserializer(msg.value(), ctx)
            if record is None:
                raise ValueError("Deserializer returned None -- tombstone or empty message")
        except Exception as e:
            self._metrics["messages_dlq"] += 1
            self._dlq.handle(
                raw_bytes=msg.value(), source_topic=topic,
                partition=msg.partition(), offset=msg.offset(),
                error=e, error_type=DLQErrorType.AVRO_DECODE_ERROR,
            )
            return

        validation_error = self._validate_record(record, topic_key)
        if validation_error:
            self._metrics["messages_dlq"] += 1
            self._dlq.handle(
                raw_bytes=msg.value(), source_topic=topic,
                partition=msg.partition(), offset=msg.offset(),
                error=ValueError(validation_error), error_type=DLQErrorType.SCHEMA_VIOLATION,
            )
            return

        record = _enrich_with_metadata(record, msg, topic_key, schema_version, is_valid=True)
        self._writer.add(topic_key, record)
        self._metrics["messages_valid"] += 1
        logger.debug(f"[OK] topic={topic} partition={msg.partition()} offset={msg.offset()}")

    def _do_flush(self) -> None:
        dlq_rows = self._dlq.flush_rows()
        self._writer.add_dlq_rows(dlq_rows)

        flush_counts = self._writer.flush()
        if flush_counts:
            self._metrics["flushes"] += 1
            logger.info(
                f"Flush #{self._metrics['flushes']} complete: {flush_counts} "
                f"| valid={self._metrics['messages_valid']} dlq={self._metrics['messages_dlq']}"
            )

        try:
            self._consumer.commit(asynchronous=False)
        except KafkaException as e:
            logger.error(f"Kafka offset commit failed: {e}")

    def _validate_record(self, record: dict, topic_key: str) -> Optional[str]:
        required_by_topic = {
            "orders":      ["order_id", "customer_id", "product_id"],
            "clickstream": ["event_id", "user_id"],
            "payments":    ["payment_id", "order_id", "customer_id"],
            "weather":     ["city"],
        }
        for f in required_by_topic.get(topic_key, []):
            val = record.get(f)
            if val is None or (isinstance(val, str) and not val.strip()):
                return f"Required field '{f}' is null or empty"

        if topic_key == "orders":
            qty   = record.get("quantity")
            price = record.get("unit_price")
            if qty is not None and qty < 0:
                return f"quantity must be >= 0, got {qty}"
            if price is not None and price < 0:
                return f"unit_price must be >= 0, got {price}"

        if topic_key == "payments":
            amount = record.get("amount")
            if amount is not None and amount < 0:
                return f"payment amount must be >= 0, got {amount}"

        return None

    def _handle_kafka_error(self, msg: Message) -> None:
        err = msg.error()
        if err.code() == KafkaError._PARTITION_EOF:
            logger.debug(f"EOF: topic={msg.topic()} partition={msg.partition()} offset={msg.offset()}")
        elif err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            logger.error(f"Unknown topic or partition: {err}")
        else:
            logger.error(f"Kafka consumer error: {err}")

    def _shutdown(self) -> None:
        logger.info("Shutting down FlinkConsumer...")
        self._do_flush()
        self._writer.flush_and_close()
        self._dlq.close()

        try:
            self._consumer.close()
            logger.info("Kafka consumer closed.")
        except Exception as e:
            logger.error(f"Error closing Kafka consumer: {e}")

        try:
            self._spark.stop()
            logger.info("SparkSession stopped.")
        except Exception as e:
            logger.error(f"Error stopping SparkSession: {e}")

        elapsed = (
            datetime.now(timezone.utc)
            - datetime.fromisoformat(self._metrics["start_time"]).replace(tzinfo=timezone.utc)
        )
        logger.info("=== Final Metrics ===")
        for k, v in self._metrics.items():
            logger.info(f"  {k}: {v}")
        logger.info(f"  elapsed_seconds: {elapsed.total_seconds():.1f}")
        logger.info(f"  bronze_write_stats: {self._writer.stats}")
        logger.info("=====================")


def main() -> None:
    consumer = FlinkConsumer()
    signal.signal(signal.SIGINT,  consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    logger.info("Starting Flink consumer -> Delta Lake Bronze writer")
    consumer.start()


if __name__ == "__main__":
    main()
