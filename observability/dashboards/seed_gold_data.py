"""
observability/dashboards/seed_gold_data.py
-------------------------------------------
Seeds SQLite with realistic Gold-layer data for Superset dev/demo.

This is only needed when S3/Delta Lake is not yet connected.
Once the Flink consumer has run and dbt Gold tables exist on S3,
replace this SQLite connection with a Spark SQL or Trino connection.

Run:
    python observability/dashboards/seed_gold_data.py
"""

import sqlite3
import random
from datetime import datetime, timedelta
from pathlib import Path

DB_PATH = Path(__file__).parent / "gold_data.db"


def seed():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    # ── gold_daily_order_revenue ───────────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS gold_daily_order_revenue")
    cur.execute("""
        CREATE TABLE gold_daily_order_revenue (
            event_date        TEXT,
            currency_code     TEXT,
            total_orders      INTEGER,
            unique_customers  INTEGER,
            gross_revenue     REAL,
            avg_unit_price    REAL,
            total_units_sold  INTEGER,
            dbt_updated_at    TEXT
        )
    """)

    currencies = ["USD", "EUR", "GBP", "JPY"]
    base_date = datetime.now() - timedelta(days=14)
    revenue_rows = []
    for day in range(14):
        date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
        for currency in currencies:
            orders  = random.randint(40, 120)
            units   = random.randint(orders, orders * 3)
            revenue = round(random.uniform(2000, 15000), 2)
            revenue_rows.append((
                date, currency, orders,
                random.randint(30, orders),
                revenue,
                round(revenue / units, 2),
                units,
                datetime.now().isoformat(),
            ))
    cur.executemany("INSERT INTO gold_daily_order_revenue VALUES (?,?,?,?,?,?,?,?)", revenue_rows)
    print(f"  gold_daily_order_revenue: {len(revenue_rows)} rows")

    # ── gold_payment_success_rate ──────────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS gold_payment_success_rate")
    cur.execute("""
        CREATE TABLE gold_payment_success_rate (
            event_date           TEXT,
            payment_method       TEXT,
            currency_code        TEXT,
            total_payments       INTEGER,
            successful_payments  INTEGER,
            failed_payments      INTEGER,
            refunded_payments    INTEGER,
            success_rate_pct     REAL,
            total_volume         REAL,
            avg_payment_amount   REAL,
            dbt_updated_at       TEXT
        )
    """)

    methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "crypto"]
    payment_rows = []
    for day in range(14):
        date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
        for method in methods:
            total    = random.randint(20, 80)
            success  = random.randint(int(total * 0.75), int(total * 0.98))
            failed   = random.randint(0, total - success)
            refunded = max(total - success - failed, 0)
            volume   = round(random.uniform(1000, 10000), 2)
            payment_rows.append((
                date, method, "USD",
                total, success, failed, refunded,
                round(success / total * 100, 2),
                volume,
                round(volume / total, 2),
                datetime.now().isoformat(),
            ))
    cur.executemany("INSERT INTO gold_payment_success_rate VALUES (?,?,?,?,?,?,?,?,?,?,?)", payment_rows)
    print(f"  gold_payment_success_rate: {len(payment_rows)} rows")

    # ── gold_pipeline_health ───────────────────────────────────────────────────
    cur.execute("DROP TABLE IF EXISTS gold_pipeline_health")
    cur.execute("""
        CREATE TABLE gold_pipeline_health (
            event_date        TEXT,
            source_topic      TEXT,
            error_type        TEXT,
            violation_count   INTEGER,
            first_seen        TEXT,
            last_seen         TEXT,
            dbt_updated_at    TEXT
        )
    """)

    topics = ["orders", "clickstream", "payments", "weather"]
    error_types = [
        "AVRO_DECODE_ERROR", "SCHEMA_VIOLATION",
        "MISSING_REQUIRED_FIELD", "INVALID_VALUE", "INTENTIONAL_MALFORMED",
    ]
    health_rows = []
    for day in range(14):
        date = (base_date + timedelta(days=day)).strftime("%Y-%m-%d")
        for topic in topics:
            for error_type in random.sample(error_types, k=random.randint(2, 3)):
                health_rows.append((
                    date, topic, error_type,
                    random.randint(1, 12),
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                    datetime.now().isoformat(),
                ))
    cur.executemany("INSERT INTO gold_pipeline_health VALUES (?,?,?,?,?,?,?)", health_rows)
    print(f"  gold_pipeline_health: {len(health_rows)} rows")

    conn.commit()
    conn.close()
    print(f"\n  Database: {DB_PATH}")
    print("  Ready for Superset connection.")


if __name__ == "__main__":
    print("Seeding Gold data into SQLite for Superset dev...")
    seed()
