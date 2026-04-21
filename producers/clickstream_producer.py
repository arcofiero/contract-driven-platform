"""Clickstream Kafka producer with ~5% malformed event injection."""
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

TOPIC = os.getenv("TOPIC_CLICKSTREAM", "clickstream")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dead-letter-queue")
MALFORMED_RATE = float(os.getenv("MALFORMED_RATE", "0.05"))
EVENTS_PER_SECOND = float(os.getenv("CLICKSTREAM_EVENTS_PER_SECOND", "5"))

EVENT_TYPES = ["page_view", "button_click", "form_submit", "search", "purchase", "logout"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/account", "/search"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
DEVICES = ["desktop", "mobile", "tablet"]
COUNTRIES = ["US", "GB", "DE", "JP", "CA", "FR", "AU", "IN"]


def make_valid_clickstream() -> dict:
    session_id = str(uuid.uuid4())
    return {
        "event_id": str(uuid.uuid4()),
        "session_id": session_id,
        "user_id": f"USR-{random.randint(10000, 99999)}",
        "event_type": random.choice(EVENT_TYPES),
        "page_url": f"https://shop.example.com{random.choice(PAGES)}",
        "referrer_url": random.choice([
            "https://google.com", "https://bing.com", None, "https://shop.example.com/home"
        ]),
        "browser": random.choice(BROWSERS),
        "device_type": random.choice(DEVICES),
        "country_code": random.choice(COUNTRIES),
        "session_duration_ms": random.randint(100, 300_000),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
    }


# ── Fault factories ────────────────────────────────────────────────────────────

def _fault_missing_event_id(base: dict) -> dict:
    e = {k: v for k, v in base.items() if k != "event_id"}
    e["_fault"] = "missing_event_id"
    return e


def _fault_invalid_event_type(base: dict) -> dict:
    e = dict(base)
    e["event_type"] = "DOES_NOT_EXIST"
    e["_fault"] = "invalid_event_type"
    return e


def _fault_negative_duration(base: dict) -> dict:
    e = dict(base)
    e["session_duration_ms"] = -random.randint(1, 9999)
    e["_fault"] = "negative_session_duration"
    return e


def _fault_malformed_url(base: dict) -> dict:
    e = dict(base)
    e["page_url"] = "not-a-valid-url-$$##"
    e["_fault"] = "malformed_page_url"
    return e


def _fault_bad_country_code(base: dict) -> dict:
    e = dict(base)
    e["country_code"] = "ZZZZ"
    e["_fault"] = "invalid_country_code"
    return e


def _fault_null_session_id(base: dict) -> dict:
    e = dict(base)
    e["session_id"] = None
    e["_fault"] = "null_session_id"
    return e


def _fault_wrong_type(base: dict) -> str:  # type: ignore[return]
    return f"MALFORMED:{json.dumps(base)}"


_FAULTS = [
    _fault_missing_event_id,
    _fault_invalid_event_type,
    _fault_negative_duration,
    _fault_malformed_url,
    _fault_bad_country_code,
    _fault_null_session_id,
    _fault_wrong_type,
]


def make_malformed_clickstream():
    base = make_valid_clickstream()
    return random.choice(_FAULTS)(base)


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    producer = build_producer()
    interval = 1.0 / EVENTS_PER_SECOND
    logger.info("Clickstream producer started | topic=%s rate=%.1f/s malformed=%.0f%%",
                TOPIC, EVENTS_PER_SECOND, MALFORMED_RATE * 100)

    try:
        while True:
            is_malformed = random.random() < MALFORMED_RATE
            event = make_malformed_clickstream() if is_malformed else make_valid_clickstream()

            if isinstance(event, str):
                payload = event.encode()
                target = TOPIC_DLQ
            else:
                payload = json.dumps(event).encode()
                target = TOPIC_DLQ if is_malformed else TOPIC

            event_id = event.get("event_id", "UNKNOWN") if isinstance(event, dict) else "UNKNOWN"
            safe_produce(producer, target, payload, key=event_id.encode())

            if is_malformed:
                fault = event.get("_fault", "wrong_type") if isinstance(event, dict) else "wrong_type"
                logger.warning("Malformed clickstream | fault=%s target=%s", fault, target)

            time.sleep(interval)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)
        logger.info("Clickstream producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
