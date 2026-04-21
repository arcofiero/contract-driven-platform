"""
Clickstream Kafka producer.

Valid events    -> Avro-serialized -> topic 'clickstream'
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

TOPIC             = os.getenv("TOPIC_CLICKSTREAM", "clickstream")
TOPIC_DLQ         = os.getenv("TOPIC_DLQ",        "dead-letter-queue")
MALFORMED_RATE    = float(os.getenv("MALFORMED_RATE",               "0.05"))
EVENTS_PER_SECOND = float(os.getenv("CLICKSTREAM_EVENTS_PER_SECOND", "5"))

EVENT_TYPES = ["page_view", "button_click", "form_submit", "search", "purchase", "logout"]
PAGES       = ["/home", "/products", "/cart", "/checkout", "/account", "/search"]
BROWSERS    = ["Chrome", "Firefox", "Safari", "Edge"]
DEVICES     = ["desktop", "mobile", "tablet"]
COUNTRIES   = ["US", "GB", "DE", "JP", "CA", "FR", "AU", "IN"]
REFERRERS   = ["https://google.com", "https://bing.com", None, "https://shop.example.com/home"]


def make_valid_clickstream() -> dict:
    return {
        "event_id":            str(uuid.uuid4()),
        "session_id":          str(uuid.uuid4()),
        "user_id":             f"USR-{random.randint(10000, 99999)}",
        "event_type":          random.choice(EVENT_TYPES),
        "page_url":            f"https://shop.example.com{random.choice(PAGES)}",
        "referrer_url":        random.choice(REFERRERS),
        "browser":             random.choice(BROWSERS),
        "device_type":         random.choice(DEVICES),
        "country_code":        random.choice(COUNTRIES),
        "session_duration_ms": random.randint(100, 300_000),
        "event_timestamp":     datetime.now(timezone.utc).isoformat(),
        "schema_version":      "1.0",
    }


def _fault_missing_event_id(b):
    e = {k: v for k, v in b.items() if k != "event_id"}; e["_fault"] = "missing_event_id"; return e

def _fault_invalid_event_type(b):
    e = dict(b); e["event_type"] = "DOES_NOT_EXIST"; e["_fault"] = "invalid_event_type"; return e

def _fault_negative_duration(b):
    e = dict(b); e["session_duration_ms"] = -random.randint(1, 9999)
    e["_fault"] = "negative_session_duration"; return e

def _fault_malformed_url(b):
    e = dict(b); e["page_url"] = "not-a-valid-url-$$##"; e["_fault"] = "malformed_page_url"; return e

def _fault_bad_country_code(b):
    e = dict(b); e["country_code"] = "ZZZZ"; e["_fault"] = "invalid_country_code"; return e

def _fault_null_session_id(b):
    e = dict(b); e["session_id"] = None; e["_fault"] = "null_session_id"; return e

def _fault_wrong_type(b):
    return f"MALFORMED:{json.dumps(b)}"

_FAULTS = [_fault_missing_event_id, _fault_invalid_event_type, _fault_negative_duration,
           _fault_malformed_url, _fault_bad_country_code, _fault_null_session_id, _fault_wrong_type]


def make_malformed_clickstream():
    return random.choice(_FAULTS)(make_valid_clickstream())


def run() -> None:
    producer   = build_producer()
    serializer = get_avro_serializer("clickstream")
    interval   = 1.0 / EVENTS_PER_SECOND
    logger.info("Clickstream producer started | topic=%s rate=%.1f/s malformed=%.0f%%",
                TOPIC, EVENTS_PER_SECOND, MALFORMED_RATE * 100)
    try:
        while True:
            is_malformed = random.random() < MALFORMED_RATE
            if is_malformed:
                event   = make_malformed_clickstream()
                payload = (event if isinstance(event, str) else json.dumps(event)).encode()
                fault   = event.get("_fault", "wrong_type") if isinstance(event, dict) else "wrong_type"
                safe_produce(producer, TOPIC_DLQ, payload, key=b"MALFORMED",
                             headers={"fault": fault.encode(), "source_topic": TOPIC.encode()})
                logger.warning("Malformed clickstream -> DLQ | fault=%s", fault)
            else:
                event = make_valid_clickstream()
                safe_produce_avro(producer, TOPIC, serializer, event,
                                  key=event["event_id"].encode())
            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)
        logger.info("Clickstream producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
