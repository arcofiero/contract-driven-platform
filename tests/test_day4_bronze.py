"""
tests/test_day4_bronze.py
--------------------------
Unit tests for Day 4. No Spark or live Kafka required.

Run: pytest tests/test_day4_bronze.py -v
"""

import os
import pytest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# Patch env before config imports
os.environ.setdefault("CONFLUENT_BOOTSTRAP_SERVERS",          "localhost:9092")
os.environ.setdefault("CONFLUENT_API_KEY",                    "test_key")
os.environ.setdefault("CONFLUENT_API_SECRET",                 "test_secret")
os.environ.setdefault("CONFLUENT_SCHEMA_REGISTRY_URL",        "http://localhost:8081")
os.environ.setdefault("CONFLUENT_SCHEMA_REGISTRY_API_KEY",    "sr_key")
os.environ.setdefault("CONFLUENT_SCHEMA_REGISTRY_API_SECRET", "sr_secret")
os.environ.setdefault("AWS_ACCESS_KEY_ID",                    "test_aws_key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY",                "test_aws_secret")
os.environ.setdefault("S3_BUCKET",                            "test-bucket")


class TestExtractEventDate:
    def test_valid_epoch_ms(self):
        from flink.flink_consumer import _extract_event_date
        ts_ms = int(datetime(2024, 6, 15, tzinfo=timezone.utc).timestamp() * 1000)
        assert _extract_event_date({"event_ts": ts_ms}, "orders") == "2024-06-15"

    def test_missing_ts_falls_back_to_today(self):
        from flink.flink_consumer import _extract_event_date
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert _extract_event_date({}, "orders") == today

    def test_invalid_ts_falls_back(self):
        from flink.flink_consumer import _extract_event_date
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        assert _extract_event_date({"event_ts": "bad"}, "orders") == today


class TestEnrichWithMetadata:
    def _mock_msg(self, topic="orders", partition=0, offset=42):
        msg = MagicMock()
        msg.topic.return_value     = topic
        msg.partition.return_value = partition
        msg.offset.return_value    = offset
        return msg

    def test_all_metadata_fields_injected(self):
        from flink.flink_consumer import _enrich_with_metadata
        record = {"order_id": "ord-001", "event_ts": None}
        msg    = self._mock_msg(topic="orders", partition=1, offset=99)
        result = _enrich_with_metadata(record, msg, "orders", "1", is_valid=True)
        assert result["_source_topic"]    == "orders"
        assert result["_kafka_partition"] == 1
        assert result["_kafka_offset"]    == 99
        assert result["_schema_version"]  == "1"
        assert result["_is_valid"]        is True
        assert "_ingested_at" in result
        assert "event_date"   in result

    def test_invalid_flag(self):
        from flink.flink_consumer import _enrich_with_metadata
        msg    = self._mock_msg()
        result = _enrich_with_metadata({}, msg, "orders", None, is_valid=False)
        assert result["_is_valid"] is False


class TestValidateRecord:
    def _get_validator(self):
        from flink.flink_consumer import FlinkConsumer
        return object.__new__(FlinkConsumer)._validate_record

    def test_valid_order(self):
        v = self._get_validator()
        assert v({"order_id": "o1", "customer_id": "c1", "product_id": "p1"}, "orders") is None

    def test_missing_order_id(self):
        v = self._get_validator()
        assert "order_id" in v({"customer_id": "c1", "product_id": "p1"}, "orders")

    def test_null_order_id(self):
        v = self._get_validator()
        assert v({"order_id": None, "customer_id": "c1", "product_id": "p1"}, "orders") is not None

    def test_empty_string_order_id(self):
        v = self._get_validator()
        assert v({"order_id": "   ", "customer_id": "c1", "product_id": "p1"}, "orders") is not None

    def test_negative_quantity(self):
        v = self._get_validator()
        r = {"order_id": "o1", "customer_id": "c1", "product_id": "p1", "quantity": -1, "unit_price": 10.0}
        assert "quantity" in v(r, "orders")

    def test_negative_price(self):
        v = self._get_validator()
        r = {"order_id": "o1", "customer_id": "c1", "product_id": "p1", "quantity": 1, "unit_price": -1.0}
        assert "unit_price" in v(r, "orders")

    def test_negative_payment_amount(self):
        v = self._get_validator()
        r = {"payment_id": "p1", "order_id": "o1", "customer_id": "c1", "amount": -50.0}
        assert "amount" in v(r, "payments")

    def test_valid_clickstream(self):
        v = self._get_validator()
        assert v({"event_id": "e1", "user_id": "u1"}, "clickstream") is None

    def test_valid_weather(self):
        v = self._get_validator()
        assert v({"city": "Delhi"}, "weather") is None


class TestDLQHandler:
    @patch("flink.dlq_handler.Producer")
    def test_handle_queues_row_and_produces(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        from flink.dlq_handler import DLQHandler, DLQErrorType
        handler = DLQHandler()
        try:
            raise ValueError("test error")
        except ValueError as e:
            handler.handle(
                raw_bytes=b'{"bad": "payload"}', source_topic="orders",
                partition=0, offset=10, error=e, error_type=DLQErrorType.SCHEMA_VIOLATION,
            )
        assert handler.total_sent == 1
        rows = handler.flush_rows()
        assert len(rows) == 1
        assert rows[0]["error_type"]    == "SCHEMA_VIOLATION"
        assert rows[0]["source_topic"]  == "orders"
        assert rows[0]["_is_valid"]     is False
        assert rows[0]["_kafka_offset"] == 10

    @patch("flink.dlq_handler.Producer")
    def test_flush_clears_buffer(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        from flink.dlq_handler import DLQHandler, DLQErrorType
        handler = DLQHandler()
        for i in range(3):
            try:
                raise RuntimeError(f"err {i}")
            except RuntimeError as e:
                handler.handle(
                    raw_bytes=b"x", source_topic="payments",
                    partition=0, offset=i, error=e, error_type=DLQErrorType.UNKNOWN,
                )
        assert len(handler.flush_rows()) == 3
        assert len(handler.flush_rows()) == 0

    @patch("flink.dlq_handler.Producer")
    def test_none_raw_bytes_handled_gracefully(self, mock_producer_cls):
        mock_producer_cls.return_value = MagicMock()
        from flink.dlq_handler import DLQHandler, DLQErrorType
        handler = DLQHandler()
        try:
            raise ValueError("tombstone")
        except ValueError as e:
            handler.handle(
                raw_bytes=None, source_topic="clickstream",
                partition=2, offset=99, error=e, error_type=DLQErrorType.AVRO_DECODE_ERROR,
            )
        assert handler.flush_rows()[0]["raw_payload"] is None


class TestBronzeSchemaRegistry:
    def test_all_five_topics_present(self):
        from bronze.bronze_schema import BRONZE_TABLE_REGISTRY
        assert set(BRONZE_TABLE_REGISTRY.keys()) == {"orders", "clickstream", "payments", "weather", "dlq"}

    def test_each_topic_has_required_keys(self):
        from bronze.bronze_schema import BRONZE_TABLE_REGISTRY
        for topic, entry in BRONZE_TABLE_REGISTRY.items():
            assert "schema" in entry
            assert "delta_path" in entry
            assert "partition_cols" in entry

    def test_event_date_partition_key_in_every_schema(self):
        from bronze.bronze_schema import BRONZE_TABLE_REGISTRY
        for topic, entry in BRONZE_TABLE_REGISTRY.items():
            names = [f.name for f in entry["schema"].fields]
            assert "event_date" in names, f"{topic} missing event_date"

    def test_all_metadata_fields_present(self):
        from bronze.bronze_schema import BRONZE_TABLE_REGISTRY
        required = {"_ingested_at", "_source_topic", "_kafka_partition", "_kafka_offset", "_is_valid"}
        for topic, entry in BRONZE_TABLE_REGISTRY.items():
            names = {f.name for f in entry["schema"].fields}
            missing = required - names
            assert not missing, f"{topic} missing metadata fields: {missing}"
