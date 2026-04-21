"""Weather Kafka producer — polls OpenWeatherMap for 10 cities with ~5% fault injection."""
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv

load_dotenv()

from producers.base_producer import build_producer, safe_produce  # noqa: E402

logger = logging.getLogger(__name__)

TOPIC = os.getenv("TOPIC_WEATHER", "weather")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "dead-letter-queue")
MALFORMED_RATE = float(os.getenv("MALFORMED_RATE", "0.05"))
POLL_INTERVAL = float(os.getenv("WEATHER_POLL_INTERVAL_SECONDS", "60"))
OWM_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "")
OWM_BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

CITIES = [
    {"name": "New York", "country": "US"},
    {"name": "London", "country": "GB"},
    {"name": "Tokyo", "country": "JP"},
    {"name": "Berlin", "country": "DE"},
    {"name": "Sydney", "country": "AU"},
    {"name": "Toronto", "country": "CA"},
    {"name": "Paris", "country": "FR"},
    {"name": "Mumbai", "country": "IN"},
    {"name": "Singapore", "country": "SG"},
    {"name": "Dubai", "country": "AE"},
]


def _fetch_weather(city: str, country: str) -> dict:
    """Fetch live weather from OpenWeatherMap. Falls back to synthetic data on error."""
    if not OWM_API_KEY:
        return _synthetic_weather(city, country)
    try:
        resp = requests.get(
            OWM_BASE_URL,
            params={"q": f"{city},{country}", "appid": OWM_API_KEY, "units": "metric"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        return {
            "event_id": str(uuid.uuid4()),
            "city": city,
            "country_code": country,
            "temperature_c": data["main"]["temp"],
            "feels_like_c": data["main"]["feels_like"],
            "humidity_pct": data["main"]["humidity"],
            "wind_speed_ms": data["wind"]["speed"],
            "wind_direction_deg": data["wind"].get("deg", 0),
            "weather_condition": data["weather"][0]["main"],
            "weather_description": data["weather"][0]["description"],
            "visibility_m": data.get("visibility", 10000),
            "pressure_hpa": data["main"]["pressure"],
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "schema_version": "1.0",
        }
    except Exception as exc:
        logger.warning("OWM fetch failed for %s: %s — using synthetic data", city, exc)
        return _synthetic_weather(city, country)


def _synthetic_weather(city: str, country: str) -> dict:
    conditions = ["Clear", "Clouds", "Rain", "Snow", "Thunderstorm", "Mist"]
    return {
        "event_id": str(uuid.uuid4()),
        "city": city,
        "country_code": country,
        "temperature_c": round(random.uniform(-10.0, 40.0), 1),
        "feels_like_c": round(random.uniform(-12.0, 42.0), 1),
        "humidity_pct": random.randint(20, 100),
        "wind_speed_ms": round(random.uniform(0.0, 30.0), 1),
        "wind_direction_deg": random.randint(0, 359),
        "weather_condition": random.choice(conditions),
        "weather_description": random.choice(conditions).lower(),
        "visibility_m": random.randint(500, 10000),
        "pressure_hpa": random.randint(980, 1040),
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "schema_version": "1.0",
    }


def make_valid_weather(city: str, country: str) -> dict:
    return _fetch_weather(city, country)


# ── Fault factories ────────────────────────────────────────────────────────────

def _fault_missing_city(base: dict) -> dict:
    e = {k: v for k, v in base.items() if k != "city"}
    e["_fault"] = "missing_city"
    return e


def _fault_temperature_out_of_range(base: dict) -> dict:
    e = dict(base)
    e["temperature_c"] = random.choice([-999.9, 999.9])
    e["_fault"] = "temperature_out_of_range"
    return e


def _fault_invalid_humidity(base: dict) -> dict:
    e = dict(base)
    e["humidity_pct"] = random.choice([-5, 150])
    e["_fault"] = "invalid_humidity_pct"
    return e


def _fault_negative_wind_speed(base: dict) -> dict:
    e = dict(base)
    e["wind_speed_ms"] = -round(random.uniform(1.0, 50.0), 1)
    e["_fault"] = "negative_wind_speed"
    return e


def _fault_bad_country_code(base: dict) -> dict:
    e = dict(base)
    e["country_code"] = "XYZZY"
    e["_fault"] = "invalid_country_code"
    return e


def _fault_null_event_id(base: dict) -> dict:
    e = dict(base)
    e["event_id"] = None
    e["_fault"] = "null_event_id"
    return e


def _fault_wrong_type(base: dict) -> str:  # type: ignore[return]
    return f"MALFORMED:{json.dumps(base)}"


_FAULTS = [
    _fault_missing_city,
    _fault_temperature_out_of_range,
    _fault_invalid_humidity,
    _fault_negative_wind_speed,
    _fault_bad_country_code,
    _fault_null_event_id,
    _fault_wrong_type,
]


def make_malformed_weather(city: str, country: str):
    base = make_valid_weather(city, country)
    return random.choice(_FAULTS)(base)


# ── Main loop ──────────────────────────────────────────────────────────────────

def run():
    producer = build_producer()
    logger.info("Weather producer started | topic=%s cities=%d poll_interval=%.0fs malformed=%.0f%%",
                TOPIC, len(CITIES), POLL_INTERVAL, MALFORMED_RATE * 100)

    try:
        while True:
            for city_cfg in CITIES:
                city, country = city_cfg["name"], city_cfg["country"]
                is_malformed = random.random() < MALFORMED_RATE

                event = (make_malformed_weather(city, country) if is_malformed
                         else make_valid_weather(city, country))

                if isinstance(event, str):
                    payload = event.encode()
                    target = TOPIC_DLQ
                else:
                    payload = json.dumps(event).encode()
                    target = TOPIC_DLQ if is_malformed else TOPIC

                key = city.replace(" ", "_").encode()
                safe_produce(producer, target, payload, key=key)

                if is_malformed:
                    fault = event.get("_fault", "wrong_type") if isinstance(event, dict) else "wrong_type"
                    logger.warning("Malformed weather | city=%s fault=%s target=%s",
                                   city, fault, target)
                else:
                    logger.debug("Weather event | city=%s target=%s", city, target)

            producer.flush(timeout=10)
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush(timeout=30)
        logger.info("Weather producer stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    run()
