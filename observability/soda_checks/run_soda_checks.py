"""
observability/soda_checks/run_soda_checks.py
--------------------------------------------
Runs Soda Core contract checks against Delta Lake tables via SparkSession.

Runs against LOCAL Delta tables by default.
Set USE_S3=true in .env to run against S3.

Usage:
    python observability/soda_checks/run_soda_checks.py --layer bronze
    python observability/soda_checks/run_soda_checks.py --layer silver
    python observability/soda_checks/run_soda_checks.py --layer gold
    python observability/soda_checks/run_soda_checks.py --layer all
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from soda.scan import Scan

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("soda_runner")

CHECKS_DIR = Path(__file__).parent

# ── Storage paths (mirrors config/flink_config.py logic) ─────────────────────
USE_S3 = os.getenv("USE_S3", "false").lower() == "true"

if USE_S3:
    _bucket   = os.environ["S3_BUCKET"]
    DELTA_BASE = f"s3a://{_bucket}/delta"
else:
    _local    = os.getenv("LOCAL_DELTA_PATH", "/tmp/contract-driven-platform")
    DELTA_BASE = f"{_local}/delta"

logger.info(f"Storage mode: {'S3' if USE_S3 else 'local'} | Delta base: {DELTA_BASE}")

# ── Table path registry ───────────────────────────────────────────────────────
TABLE_PATHS = {
    "bronze_orders":      f"{DELTA_BASE}/bronze/orders",
    "bronze_clickstream": f"{DELTA_BASE}/bronze/clickstream",
    "bronze_payments":    f"{DELTA_BASE}/bronze/payments",
    "bronze_weather":     f"{DELTA_BASE}/bronze/weather",
    "bronze_dlq":         f"{DELTA_BASE}/bronze/dlq",
    "silver_orders":      f"{DELTA_BASE}/silver.db/silver_orders",
    "silver_clickstream": f"{DELTA_BASE}/silver.db/silver_clickstream",
    "silver_payments":    f"{DELTA_BASE}/silver.db/silver_payments",
    "silver_weather":     f"{DELTA_BASE}/silver.db/silver_weather",
    "gold_daily_order_revenue":  f"{DELTA_BASE}/gold.db/gold_daily_order_revenue",
    "gold_payment_success_rate": f"{DELTA_BASE}/gold.db/gold_payment_success_rate",
    "gold_pipeline_health":      f"{DELTA_BASE}/gold.db/gold_pipeline_health",
}

LAYER_CHECKS = {
    "bronze": [
        ("bronze_orders",      "bronze_orders_checks.yml"),
        ("bronze_clickstream", "bronze_clickstream_checks.yml"),
        ("bronze_payments",    "bronze_payments_checks.yml"),
        ("bronze_weather",     "bronze_weather_checks.yml"),
        ("bronze_dlq",         "bronze_dlq_checks.yml"),
    ],
    "silver": [
        ("silver_orders",      "silver_orders_checks.yml"),
        ("silver_clickstream", "silver_clickstream_checks.yml"),
        ("silver_payments",    "silver_payments_checks.yml"),
        ("silver_weather",     "silver_weather_checks.yml"),
    ],
    "gold": [
        ("gold_daily_order_revenue",  "gold_daily_order_revenue_checks.yml"),
        ("gold_payment_success_rate", "gold_payment_success_rate_checks.yml"),
        ("gold_pipeline_health",      "gold_pipeline_health_checks.yml"),
    ],
}


def build_spark() -> SparkSession:
    conf = {
        "spark.app.name": "soda-contract-checks",
        "spark.sql.extensions":
            "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog":
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.shuffle.partitions": "8",
    }

    if USE_S3:
        conf.update({
            "spark.hadoop.fs.s3a.impl":
                "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.access.key":
                os.environ["AWS_ACCESS_KEY_ID"],
            "spark.hadoop.fs.s3a.secret.key":
                os.environ["AWS_SECRET_ACCESS_KEY"],
            "spark.hadoop.fs.s3a.endpoint":
                f"s3.{os.getenv('AWS_REGION', 'us-east-1')}.amazonaws.com",
        })

    builder = SparkSession.builder
    for k, v in conf.items():
        builder = builder.config(k, v)

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_checks_for_layer(spark: SparkSession, layer: str) -> dict[str, bool]:
    checks  = LAYER_CHECKS.get(layer, [])
    results = {}

    for dataset_name, checks_file in checks:
        path        = TABLE_PATHS[dataset_name]
        checks_path = CHECKS_DIR / checks_file

        logger.info(f"Running checks: {checks_file}")
        logger.info(f"  Table path: {path}")

        try:
            df = spark.read.format("delta").load(path)
        except Exception as e:
            logger.error(f"  Could not load table {path}: {e}")
            results[checks_file] = False
            continue

        scan = Scan()
        scan.set_scan_definition_name(dataset_name)
        scan.set_data_source_name("delta_lake")
        scan.add_spark_session(spark, data_source_name="delta_lake")
        scan.add_sodacl_yaml_file(str(checks_path))
        df.createOrReplaceTempView(dataset_name)
        scan.execute()

        passed = not scan.has_check_fails()
        results[checks_file] = passed

        if scan.has_check_fails():
            for txt in (scan.get_checks_fail_text() or "").splitlines():
                if txt.strip():
                    logger.info(f"  FAIL: {txt.strip()}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Run Soda Core contract checks")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        required=True,
    )
    args   = parser.parse_args()
    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]

    spark       = build_spark()
    all_results = {}

    for layer in layers:
        logger.info(f"\n{'='*50}")
        logger.info(f"  Running {layer.upper()} layer checks")
        logger.info(f"{'='*50}")
        all_results.update(run_checks_for_layer(spark, layer))

    passed = sum(1 for v in all_results.values() if v)
    failed = sum(1 for v in all_results.values() if not v)
    total  = len(all_results)

    logger.info(f"\n{'='*50}")
    logger.info(f"  SODA CHECKS SUMMARY")
    logger.info(f"  Passed: {passed}/{total} | Failed: {failed}/{total}")
    logger.info(f"{'='*50}")
    for f, r in all_results.items():
        logger.info(f"  {'OK' if r else 'FAIL'} {f}")

    spark.stop()
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
