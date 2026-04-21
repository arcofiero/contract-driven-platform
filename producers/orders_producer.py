"""Orders Kafka producer with ~5% malformed event injection."""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone, timedelta

from dotenv import load_dotenv

load_dotenv()

from producers.base_producer import build_producer, safe_produce  # noqa: E402

logger = logging.getLogger(__name__)

TOPIC = os.getenv("TOPIC_ORDERS", "orders")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dead-letter-queue")
MALFORMED_RATE = float(os.getenv("MALFORMED_RATE", "0.05"))
EVENTS_PER_SECOND = float(os.getenv("ORDERS_EVENTS_PER_SECOND", "2"))

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
STATUSES = ["created", "pending", "confirmed", "shipped", "cancelled"]
PRODUCTS = [
    {"product_id": "PROD-001", "name": "Laptop Pro", "unit_price": 1299.99},
    {"product_id": "PROD-002", "name": "Wireless Mouse", "unit_price": 29.99},
    {"product_id": "PROD-003", "name": "USB-C Hub", "unit_price": 49.99},
    {"product_id": "PROD-004", "name": "Mechanical Keyboard", "unit_price": 149.99},
    {"product_id": "PROD-005", "name": "Monitor 4K", "unit_price": 599.99},
]


def make_valid_order() -> dict:
    items = random.sample(PRODUCTS, k=random.randint(1, 3))
    items = [dict(i) for i in items]
    for item in items:
        item["quantity"] = random.randint(1, 5)
        item["subtotal"] = round(item["unit_price"] * item["quantity"], 2)
    total = round(sum(i["subtotal"] for i in items), 2)
    return {
        "order_id": str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "status": random.choice(STATUSES),
        "currency": random.choice(CURRENCIES),
        "total_amount": total,
        "items": items,
        "shipping_address": {
            "street": f"{random.randint(1, 999)} Main St",
            "city": random.choice(["New York", "London", "Berlin", "Tokyo"]),
            "country": random.choice(["US", "GB", "DE", "JP"]),
        },
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
    }


# ── Fault factories ────────────────────────────────────────────────────────────

def _fault_missing_order_id(base: dict) -> dict:
    e = {k: v for k, v in base.items() if k != "order_id"}
    e["_fault"] = "missing_order_id"
    return e


def _fault_negative_quantity(base: dict) -> dict:
    e = dict(base)
    e["items"] = [dict(i, quantity=-abs(i["quantity"])) for i in e["items"]]
    e["_fault"] = "negative_quantity"
    return e


def _fault_null_total(base: dict) -> dict:
    e = dict(base)
    e["total_amount"] = None
    e["_fault"] = "null_total_amount"
    return e


def _fault_bad_currency(base: dict) -> dict:
    e = dict(base)
    e["currency"] = "INVALID"
    e["_fault"] = "invalid_currency_code"
    return e


def _fault_future_timestamp(base: dict) -> dict:
    e = dict(base)
    future = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 365))
    e["event_timestamp"] = future.isoformat()
    e["_fault"] = "future_timestamp"
    return e


def _fault_wrong_type(base: dict) -> str:  # type: ignore[return]
    return f"MALFORMED:{json.dumps(base)}"


_FAULTS = [
    _fault_missing_order_id,
    _fault_negative_quantity,
    _fault_null_total,
    _fault_bad_currency,
    _fault_future_timestamp,
    _fault_wrong_type,
]


def make_malformed_order():
    base = make_valid_order()
    return random.choice(_FAULTS)(base)


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    producer = build_producer()
    interval = 1.0 / EVENTS_PER_SECOND
    logger.info("Orders producer started | topic=%s rate=%.1f/s malformed=%.0f%%",
                TOPIC, EVENTS_PER_SECOND, MALFORMED_RATE * 100)

    try:
        while True:
            is_malformed = random.random() < MALFORMED_RATE
            event = make_malformed_order() if is_malformed else make_valid_order()

            if isinstance(event, str):
                payload = event.encode()
                target = TOPIC_DLQ
            else:
                payload = json.dumps(event).encode()
                target = TOPIC_DLQ if is_malformed else TOPIC

            order_id = event.get("order_id", "UNKNOWN") if isinstance(event, dict) else "UNKNOWN"
            safe_produce(producer, target, payload, key=order_id.encode())

            if is_malformed:
                fault = event.get("_fault", "wrong_type") if isinstance(event, dict) else "wrong_type"
                logger.warning("Malformed order | fault=%s target=%s", fault, target)

            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)
        logger.info("Orders producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
