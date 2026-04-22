"""
flink/dlq_handler.py
---------------------
Dead Letter Queue handler.

Responsibilities:
  1. Produce malformed/undeserializable events to the Confluent DLQ Kafka topic
  2. Accumulate DLQ rows for Bronze Delta Lake writes (flushed by bronze_writer)
  3. Provide structured error classification for observability
"""

import json
import logging
import traceback
from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional

from confluent_kafka import Producer, KafkaException

from config.flink_config import (
    KAFKA_CONFIG,
    TOPIC_DLQ,
    MAX_RETRIES,
    RETRY_BACKOFF_SEC,
)

logger = logging.getLogger(__name__)


class DLQErrorType(str, Enum):
    AVRO_DECODE_ERROR = "AVRO_DECODE_ERROR"
    SCHEMA_VIOLATION  = "SCHEMA_VIOLATION"
    MISSING_REQUIRED  = "MISSING_REQUIRED_FIELD"
    TYPE_MISMATCH     = "TYPE_MISMATCH"
    INVALID_VALUE     = "INVALID_VALUE"
    PRODUCER_ERROR    = "INTENTIONAL_MALFORMED"
    UNKNOWN           = "UNKNOWN_ERROR"


@dataclass
class DLQRecord:
    raw_payload:      Optional[str]
    error_message:    str
    error_type:       str
    source_topic:     str
    event_date:       str
    _ingested_at:     str
    _source_topic:    str
    _kafka_partition: int
    _kafka_offset:    int
    _schema_version:  Optional[str] = None
    _is_valid:        bool = False

    def to_dict(self) -> dict:
        return asdict(self)


class DLQHandler:
    """
    Routes bad Kafka messages to the DLQ Kafka topic and accumulates
    DLQ rows for batch Bronze Delta writes.
    """

    def __init__(self):
        producer_conf = {
            "bootstrap.servers": KAFKA_CONFIG["bootstrap.servers"],
            "security.protocol": KAFKA_CONFIG["security.protocol"],
            "sasl.mechanisms":   KAFKA_CONFIG["sasl.mechanisms"],
            "sasl.username":     KAFKA_CONFIG["sasl.username"],
            "sasl.password":     KAFKA_CONFIG["sasl.password"],
            "acks":              "all",
            "retries":           MAX_RETRIES,
            "retry.backoff.ms":  int(RETRY_BACKOFF_SEC * 1000),
        }
        self._producer = Producer(producer_conf)
        self._pending_rows: list[dict] = []
        self._total_sent = 0
        logger.info(f"DLQHandler initialized -> topic: {TOPIC_DLQ}")

    def handle(
        self,
        *,
        raw_bytes:      Optional[bytes],
        source_topic:   str,
        partition:      int,
        offset:         int,
        error:          Exception,
        error_type:     DLQErrorType = DLQErrorType.UNKNOWN,
        schema_version: Optional[str] = None,
    ) -> None:
        now_utc     = datetime.now(timezone.utc)
        event_date  = now_utc.strftime("%Y-%m-%d")
        ingested_at = now_utc.isoformat()

        raw_str = None
        if raw_bytes is not None:
            try:
                raw_str = raw_bytes.decode("utf-8", errors="replace")
            except Exception:
                raw_str = repr(raw_bytes)

        error_msg = self._format_error(error)

        record = DLQRecord(
            raw_payload=raw_str,
            error_message=error_msg,
            error_type=error_type.value,
            source_topic=source_topic,
            event_date=event_date,
            _ingested_at=ingested_at,
            _source_topic=source_topic,
            _kafka_partition=partition,
            _kafka_offset=offset,
            _schema_version=schema_version,
            _is_valid=False,
        )

        self._produce_to_kafka(record, source_topic)
        self._pending_rows.append(record.to_dict())
        self._total_sent += 1

        logger.warning(
            f"[DLQ] {error_type.value} | topic={source_topic} "
            f"partition={partition} offset={offset} | {error_msg[:120]}"
        )

    def flush_rows(self) -> list[dict]:
        """Return and clear pending DLQ rows for Bronze Delta write."""
        rows = self._pending_rows.copy()
        self._pending_rows.clear()
        return rows

    def close(self) -> None:
        try:
            remaining = self._producer.flush(timeout=10)
            if remaining > 0:
                logger.warning(f"DLQ producer: {remaining} messages not delivered on shutdown")
            else:
                logger.info(f"DLQ producer flushed. Total DLQ events sent: {self._total_sent}")
        except Exception as e:
            logger.error(f"Error flushing DLQ producer: {e}")

    @property
    def total_sent(self) -> int:
        return self._total_sent

    def _produce_to_kafka(self, record: DLQRecord, source_topic: str) -> None:
        payload = json.dumps(record.to_dict(), default=str).encode("utf-8")
        headers = {
            "dlq.source.topic":  source_topic,
            "dlq.error.type":    record.error_type,
            "dlq.error.message": record.error_message[:500],
            "dlq.ingested.at":   record._ingested_at,
        }
        try:
            self._producer.produce(
                topic=TOPIC_DLQ,
                value=payload,
                headers=[(k, v.encode("utf-8")) for k, v in headers.items()],
                on_delivery=self._delivery_callback,
            )
            self._producer.poll(0)
        except KafkaException as e:
            logger.error(f"Failed to produce DLQ message: {e}")
        except BufferError:
            logger.warning("DLQ producer buffer full, flushing...")
            self._producer.flush(timeout=5)
            try:
                self._producer.produce(topic=TOPIC_DLQ, value=payload)
            except Exception as retry_err:
                logger.error(f"DLQ retry also failed: {retry_err}")

    @staticmethod
    def _delivery_callback(err, msg) -> None:
        if err:
            logger.error(f"DLQ delivery failed | topic={msg.topic()} | error={err}")
        else:
            logger.debug(
                f"DLQ delivered | topic={msg.topic()} "
                f"partition={msg.partition()} offset={msg.offset()}"
            )

    @staticmethod
    def _format_error(error: Exception) -> str:
        tb = traceback.format_exc()
        lines = [ln for ln in tb.splitlines() if ln.strip()][-4:]
        return f"{type(error).__name__}: {error} | trace: {' <- '.join(lines)}"
