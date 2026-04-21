"""Payments Kafka producer with ~5% malformed event injection."""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv()

from producers.base_producer import build_producer, safe_produce  # noqa: E402

logger = logging.getLogger(__name__)

TOPIC = os.getenv("TOPIC_PAYMENTS", "payments")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dead-letter-queue")
MALFORMED_RATE = float(os.getenv("MALFORMED_RATE", "0.05"))
EVENTS_PER_SECOND = float(os.getenv("PAYMENTS_EVENTS_PER_SECOND", "1.5"))

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
STATUSES = ["initiated", "processing", "completed", "failed", "refunded", "disputed"]
PROCESSORS = ["Stripe", "Adyen", "Braintree", "Square", "PayPal"]


def make_valid_payment() -> dict:
    amount = round(random.uniform(5.0, 2000.0), 2)
    return {
        "payment_id": str(uuid.uuid4()),
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "amount": amount,
        "currency": random.choice(CURRENCIES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "status": random.choice(STATUSES),
        "processor": random.choice(PROCESSORS),
        "processor_transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "processing_time_ms": random.randint(50, 5000),
        "risk_score": round(random.uniform(0.0, 1.0), 4),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
    }


# ── Fault factories ────────────────────────────────────────────────────────────

def _fault_missing_payment_id(base: dict) -> dict:
    e = {k: v for k, v in base.items() if k != "payment_id"}
    e["_fault"] = "missing_payment_id"
    return e


def _fault_zero_amount(base: dict) -> dict:
    e = dict(base)
    e["amount"] = 0.0
    e["_fault"] = "zero_amount"
    return e


def _fault_negative_amount(base: dict) -> dict:
    e = dict(base)
    e["amount"] = round(-random.uniform(1.0, 500.0), 2)
    e["_fault"] = "negative_amount"
    return e


def _fault_invalid_currency(base: dict) -> dict:
    e = dict(base)
    e["currency"] = "FAKE"
    e["_fault"] = "invalid_currency"
    return e


def _fault_invalid_status(base: dict) -> dict:
    e = dict(base)
    e["status"] = "MYSTERY_STATUS"
    e["_fault"] = "invalid_payment_status"
    return e


def _fault_missing_order_id(base: dict) -> dict:
    e = {k: v for k, v in base.items() if k != "order_id"}
    e["_fault"] = "missing_order_id"
    return e


def _fault_negative_processing_time(base: dict) -> dict:
    e = dict(base)
    e["processing_time_ms"] = -random.randint(1, 9999)
    e["_fault"] = "negative_processing_time_ms"
    return e


def _fault_wrong_type(base: dict) -> str:  # type: ignore[return]
    return f"MALFORMED:{json.dumps(base)}"


_FAULTS = [
    _fault_missing_payment_id,
    _fault_zero_amount,
    _fault_negative_amount,
    _fault_invalid_currency,
    _fault_invalid_status,
    _fault_missing_order_id,
    _fault_negative_processing_time,
    _fault_wrong_type,
]


def make_malformed_payment():
    base = make_valid_payment()
    return random.choice(_FAULTS)(base)


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    producer = build_producer()
    interval = 1.0 / EVENTS_PER_SECOND
    logger.info("Payments producer started | topic=%s rate=%.1f/s malformed=%.0f%%",
                TOPIC, EVENTS_PER_SECOND, MALFORMED_RATE * 100)

    try:
        while True:
            is_malformed = random.random() < MALFORMED_RATE
            event = make_malformed_payment() if is_malformed else make_valid_payment()

            if isinstance(event, str):
                payload = event.encode()
                target = TOPIC_DLQ
            else:
                payload = json.dumps(event).encode()
                target = TOPIC_DLQ if is_malformed else TOPIC

            payment_id = event.get("payment_id", "UNKNOWN") if isinstance(event, dict) else "UNKNOWN"
            safe_produce(producer, target, payload, key=payment_id.encode())

            if is_malformed:
                fault = event.get("_fault", "wrong_type") if isinstance(event, dict) else "wrong_type"
                logger.warning("Malformed payment | fault=%s target=%s", fault, target)

            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)
        logger.info("Payments producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
