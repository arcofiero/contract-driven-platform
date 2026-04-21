"""
Payments Kafka producer.

Valid events    -> Avro-serialized -> topic 'payments'
Malformed (~5%) -> raw bytes       -> topic 'dead-letter-queue' (with 'fault' header)
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from producers.base_producer import build_producer, safe_produce, safe_produce_avro  # noqa: E402
from contracts.registry_client import get_avro_serializer  # noqa: E402

logger = logging.getLogger(__name__)

TOPIC             = os.getenv("TOPIC_PAYMENTS", "payments")
TOPIC_DLQ         = os.getenv("TOPIC_DLQ",     "dead-letter-queue")
MALFORMED_RATE    = float(os.getenv("MALFORMED_RATE",            "0.05"))
EVENTS_PER_SECOND = float(os.getenv("PAYMENTS_EVENTS_PER_SECOND", "1.5"))

CURRENCIES      = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
STATUSES        = ["initiated", "processing", "completed", "failed", "refunded", "disputed"]
PROCESSORS      = ["Stripe", "Adyen", "Braintree", "Square", "PayPal"]


def make_valid_payment() -> dict:
    amount = round(random.uniform(5.0, 2000.0), 2)
    return {
        "payment_id":              str(uuid.uuid4()),
        "order_id":                str(uuid.uuid4()),
        "customer_id":             f"CUST-{random.randint(1000, 9999)}",
        "amount":                  amount,
        "currency":                random.choice(CURRENCIES),
        "payment_method":          random.choice(PAYMENT_METHODS),
        "status":                  random.choice(STATUSES),
        "processor":               random.choice(PROCESSORS),
        "processor_transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "processing_time_ms":      random.randint(50, 5000),
        "risk_score":              round(random.uniform(0.0, 1.0), 4),
        "event_timestamp":         datetime.now(timezone.utc).isoformat(),
        "schema_version":          "1.0",
    }


def _fault_missing_payment_id(b):
    e = {k: v for k, v in b.items() if k != "payment_id"}; e["_fault"] = "missing_payment_id"; return e

def _fault_zero_amount(b):
    e = dict(b); e["amount"] = 0.0; e["_fault"] = "zero_amount"; return e

def _fault_negative_amount(b):
    e = dict(b); e["amount"] = round(-random.uniform(1.0, 500.0), 2)
    e["_fault"] = "negative_amount"; return e

def _fault_invalid_currency(b):
    e = dict(b); e["currency"] = "FAKE"; e["_fault"] = "invalid_currency"; return e

def _fault_invalid_status(b):
    e = dict(b); e["status"] = "MYSTERY_STATUS"; e["_fault"] = "invalid_payment_status"; return e

def _fault_missing_order_id(b):
    e = {k: v for k, v in b.items() if k != "order_id"}; e["_fault"] = "missing_order_id"; return e

def _fault_negative_processing_time(b):
    e = dict(b); e["processing_time_ms"] = -random.randint(1, 9999)
    e["_fault"] = "negative_processing_time_ms"; return e

def _fault_wrong_type(b):
    return f"MALFORMED:{json.dumps(b)}"

_FAULTS = [_fault_missing_payment_id, _fault_zero_amount, _fault_negative_amount,
           _fault_invalid_currency, _fault_invalid_status, _fault_missing_order_id,
           _fault_negative_processing_time, _fault_wrong_type]


def make_malformed_payment():
    return random.choice(_FAULTS)(make_valid_payment())


def run() -> None:
    producer   = build_producer()
    serializer = get_avro_serializer("payments")
    interval   = 1.0 / EVENTS_PER_SECOND
    logger.info("Payments producer started | topic=%s rate=%.1f/s malformed=%.0f%%",
                TOPIC, EVENTS_PER_SECOND, MALFORMED_RATE * 100)
    try:
        while True:
            is_malformed = random.random() < MALFORMED_RATE
            if is_malformed:
                event   = make_malformed_payment()
                payload = (event if isinstance(event, str) else json.dumps(event)).encode()
                fault   = event.get("_fault", "wrong_type") if isinstance(event, dict) else "wrong_type"
                safe_produce(producer, TOPIC_DLQ, payload, key=b"MALFORMED",
                             headers={"fault": fault.encode(), "source_topic": TOPIC.encode()})
                logger.warning("Malformed payment -> DLQ | fault=%s", fault)
            else:
                event = make_valid_payment()
                safe_produce_avro(producer, TOPIC, serializer, event,
                                  key=event["payment_id"].encode())
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)
        logger.info("Payments producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
