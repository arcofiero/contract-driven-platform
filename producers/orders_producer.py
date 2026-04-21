"""
Orders Kafka producer.

Valid events    -> Avro-serialized -> topic 'orders'
Malformed (~5%) -> raw bytes       -> topic 'dead-letter-queue' (with 'fault' header)
"""
from __future__ import annotations

import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv

load_dotenv()

from producers.base_producer import build_producer, safe_produce, safe_produce_avro  # noqa: E402
from contracts.registry_client import get_avro_serializer  # noqa: E402

logger = logging.getLogger(__name__)

TOPIC             = os.getenv("TOPIC_ORDERS", "orders")
TOPIC_DLQ         = os.getenv("TOPIC_DLQ",   "dead-letter-queue")
MALFORMED_RATE    = float(os.getenv("MALFORMED_RATE",           "0.05"))
EVENTS_PER_SECOND = float(os.getenv("ORDERS_EVENTS_PER_SECOND", "2"))

CURRENCIES = ["USD", "EUR", "GBP", "JPY", "CAD"]
STATUSES   = ["created", "pending", "confirmed", "shipped", "cancelled"]
PRODUCTS   = [
    {"product_id": "PROD-001", "name": "Laptop Pro",         "unit_price": 1299.99},
    {"product_id": "PROD-002", "name": "Wireless Mouse",      "unit_price":   29.99},
    {"product_id": "PROD-003", "name": "USB-C Hub",           "unit_price":   49.99},
    {"product_id": "PROD-004", "name": "Mechanical Keyboard", "unit_price":  149.99},
    {"product_id": "PROD-005", "name": "Monitor 4K",          "unit_price":  599.99},
]


def make_valid_order() -> dict:
    items = [dict(p) for p in random.sample(PRODUCTS, k=random.randint(1, 3))]
    for item in items:
        item["quantity"] = random.randint(1, 5)
        item["subtotal"] = round(item["unit_price"] * item["quantity"], 2)
    total = round(sum(i["subtotal"] for i in items), 2)
    return {
        "order_id":    str(uuid.uuid4()),
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "status":      random.choice(STATUSES),
        "currency":    random.choice(CURRENCIES),
        "total_amount": total,
        "items":       items,
        "shipping_address": {
            "street":  f"{random.randint(1, 999)} Main St",
            "city":    random.choice(["New York", "London", "Berlin", "Tokyo"]),
            "country": random.choice(["US", "GB", "DE", "JP"]),
        },
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version":  "1.0",
    }


def _fault_missing_order_id(b):
    e = {k: v for k, v in b.items() if k != "order_id"}; e["_fault"] = "missing_order_id"; return e

def _fault_negative_quantity(b):
    e = dict(b); e["items"] = [dict(i, quantity=-abs(i["quantity"])) for i in e["items"]]
    e["_fault"] = "negative_quantity"; return e

def _fault_null_total(b):
    e = dict(b); e["total_amount"] = None; e["_fault"] = "null_total_amount"; return e

def _fault_bad_currency(b):
    e = dict(b); e["currency"] = "INVALID"; e["_fault"] = "invalid_currency_code"; return e

def _fault_future_timestamp(b):
    e = dict(b)
    future = datetime.now(timezone.utc) + timedelta(days=random.randint(1, 365))
    e["event_timestamp"] = future.isoformat(); e["_fault"] = "future_timestamp"; return e

def _fault_wrong_type(b):
    return f"MALFORMED:{json.dumps(b)}"

_FAULTS = [_fault_missing_order_id, _fault_negative_quantity, _fault_null_total,
           _fault_bad_currency, _fault_future_timestamp, _fault_wrong_type]


def make_malformed_order():
    return random.choice(_FAULTS)(make_valid_order())


def run() -> None:
    producer   = build_producer()
    serializer = get_avro_serializer("orders")
    interval   = 1.0 / EVENTS_PER_SECOND
    logger.info("Orders producer started | topic=%s rate=%.1f/s malformed=%.0f%%",
                TOPIC, EVENTS_PER_SECOND, MALFORMED_RATE * 100)
    try:
        while True:
            is_malformed = random.random() < MALFORMED_RATE
            if is_malformed:
                event   = make_malformed_order()
                payload = (event if isinstance(event, str) else json.dumps(event)).encode()
                fault   = event.get("_fault", "wrong_type") if isinstance(event, dict) else "wrong_type"
                safe_produce(producer, TOPIC_DLQ, payload, key=b"MALFORMED",
                             headers={"fault": fault.encode(), "source_topic": TOPIC.encode()})
                logger.warning("Malformed order -> DLQ | fault=%s", fault)
            else:
                event = make_valid_order()
                safe_produce_avro(producer, TOPIC, serializer, event,
                                  key=event["order_id"].encode())
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)
        logger.info("Orders producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
