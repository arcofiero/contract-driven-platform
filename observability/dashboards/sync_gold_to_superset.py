"""
observability/dashboards/sync_gold_to_superset.py
--------------------------------------------------
Reads the real Gold Delta Lake tables (written by dbt) and syncs them
into gold_data.db (SQLite) so Superset shows live pipeline data.

Run after every `dbt run --select gold_*`:
    python observability/dashboards/sync_gold_to_superset.py

Requires Java 17 in PATH (same requirement as the Flink consumer).
"""

import os
import sqlite3
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

DB_PATH = Path(__file__).parent / "gold_data.db"

LOCAL_BASE = os.getenv("LOCAL_DELTA_PATH", "/tmp/contract-driven-platform")
DELTA_GOLD = f"{LOCAL_BASE}/delta/gold.db"

TABLES = {
    "gold_daily_order_revenue":  f"{DELTA_GOLD}/gold_daily_order_revenue",
    "gold_payment_success_rate": f"{DELTA_GOLD}/gold_payment_success_rate",
    "gold_pipeline_health":      f"{DELTA_GOLD}/gold_pipeline_health",
}


def build_spark():
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder
        .appName("sync-gold-to-superset")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def sync_table(spark, name: str, path: str, conn: sqlite3.Connection) -> int:
    df = spark.read.format("delta").load(path)
    rows = [row.asDict() for row in df.collect()]
    if not rows:
        print(f"  {name}: 0 rows (table empty — skipping)")
        return 0

    # Derive column list from the first row
    cols = list(rows[0].keys())
    placeholders = ", ".join("?" * len(cols))
    col_names = ", ".join(cols)

    cur = conn.cursor()
    cur.execute(f"DROP TABLE IF EXISTS {name}")
    # Build CREATE TABLE from Spark schema
    type_map = {col.name: col.dataType.simpleString() for col in df.schema}
    sql_types = []
    for c in cols:
        t = type_map.get(c, "string")
        if "int" in t or "long" in t:
            sql_types.append(f"{c} INTEGER")
        elif "double" in t or "float" in t or "decimal" in t:
            sql_types.append(f"{c} REAL")
        else:
            sql_types.append(f"{c} TEXT")
    cur.execute(f"CREATE TABLE {name} ({', '.join(sql_types)})")

    # Coerce non-SQLite-native types to string
    def _coerce(v):
        if v is None:
            return None
        if isinstance(v, (int, float, str)):
            return v
        return str(v)

    data = [tuple(_coerce(row[c]) for c in cols) for row in rows]
    cur.executemany(f"INSERT INTO {name} ({col_names}) VALUES ({placeholders})", data)
    conn.commit()
    print(f"  {name}: {len(rows)} rows synced")
    return len(rows)


def main():
    print("Syncing Gold Delta tables → gold_data.db for Superset...")
    print(f"  Delta source: {DELTA_GOLD}")
    print(f"  SQLite target: {DB_PATH}\n")

    spark = build_spark()
    conn = sqlite3.connect(DB_PATH)

    total = 0
    failed = []
    for name, path in TABLES.items():
        try:
            total += sync_table(spark, name, path, conn)
        except Exception as e:
            print(f"  {name}: ERROR — {e}")
            failed.append(name)

    conn.close()
    spark.stop()

    print(f"\nDone. {total} total rows written to {DB_PATH}")
    if failed:
        print(f"Failed tables: {failed}")
        sys.exit(1)
    print("Superset will now show real pipeline data on next dashboard refresh.")


if __name__ == "__main__":
    main()
