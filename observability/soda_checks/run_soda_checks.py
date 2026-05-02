"""
observability/soda_checks/run_soda_checks.py
--------------------------------------------
Runs all Soda Core checks against Delta Lake tables via SparkSession.

Usage:
    # Run all checks for a specific layer
    python observability/soda_checks/run_soda_checks.py --layer bronze
    python observability/soda_checks/run_soda_checks.py --layer silver
    python observability/soda_checks/run_soda_checks.py --layer gold

    # Run all layers
    python observability/soda_checks/run_soda_checks.py --layer all

This script:
  1. Builds a SparkSession with Delta Lake + S3A config
  2. Reads each Delta table into a Spark DataFrame
  3. Registers it as a Soda scan data source
  4. Runs the corresponding YAML checks file
  5. Prints pass/fail summary and exits non-zero if any check fails
     (non-zero exit allows Airflow to detect check failures on Day 7)
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
S3_BUCKET = os.environ["S3_BUCKET"]
DELTA_BASE = f"s3a://{S3_BUCKET}/delta"

# Map: soda dataset name -> Delta table S3 path
TABLE_PATHS = {
    # Bronze
    "bronze_orders":      f"{DELTA_BASE}/bronze/orders",
    "bronze_clickstream": f"{DELTA_BASE}/bronze/clickstream",
    "bronze_payments":    f"{DELTA_BASE}/bronze/payments",
    "bronze_weather":     f"{DELTA_BASE}/bronze/weather",
    "bronze_dlq":         f"{DELTA_BASE}/bronze/dlq",
    # Silver
    "silver_orders":      f"{DELTA_BASE}/silver/orders",
    "silver_clickstream": f"{DELTA_BASE}/silver/clickstream",
    "silver_payments":    f"{DELTA_BASE}/silver/payments",
    "silver_weather":     f"{DELTA_BASE}/silver/weather",
    # Gold
    "gold_daily_order_revenue":  f"{DELTA_BASE}/gold/daily_order_revenue",
    "gold_payment_success_rate": f"{DELTA_BASE}/gold/payment_success_rate",
    "gold_pipeline_health":      f"{DELTA_BASE}/gold/pipeline_health",
}

# Map: layer name -> list of (dataset_name, checks_file)
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
    builder = (
        SparkSession.builder
        .appName("soda-contract-checks")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{os.getenv('AWS_REGION', 'us-east-1')}.amazonaws.com",
        )
        .config("spark.sql.shuffle.partitions", "8")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_checks_for_layer(spark: SparkSession, layer: str) -> dict[str, bool]:
    checks = LAYER_CHECKS.get(layer, [])
    results = {}

    for dataset_name, checks_file in checks:
        path = TABLE_PATHS[dataset_name]
        checks_path = CHECKS_DIR / checks_file

        logger.info(f"Running checks: {checks_file} against {path}")

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
        scan.add_variables({"dataset": dataset_name})

        df.createOrReplaceTempView(dataset_name)

        scan.execute()

        passed = not scan.has_check_failures()
        results[checks_file] = passed

        for check_result in scan.get_checks():
            status = "✅ PASS" if str(check_result.outcome) == "passed" else "❌ FAIL"
            logger.info(f"  {status}: {check_result.check.name}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Run Soda Core contract checks")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold", "all"],
        required=True,
        help="Which medallion layer to check",
    )
    args = parser.parse_args()

    layers = ["bronze", "silver", "gold"] if args.layer == "all" else [args.layer]

    spark = build_spark()
    all_results: dict[str, bool] = {}

    for layer in layers:
        logger.info(f"\n{'='*50}")
        logger.info(f"  Running {layer.upper()} layer checks")
        logger.info(f"{'='*50}")
        layer_results = run_checks_for_layer(spark, layer)
        all_results.update(layer_results)

    passed = sum(1 for v in all_results.values() if v)
    failed = sum(1 for v in all_results.values() if not v)
    total  = len(all_results)

    logger.info(f"\n{'='*50}")
    logger.info(f"  SODA CHECKS SUMMARY")
    logger.info(f"  Passed: {passed}/{total}  |  Failed: {failed}/{total}")
    logger.info(f"{'='*50}")

    for check_file, result in all_results.items():
        icon = "✅" if result else "❌"
        logger.info(f"  {icon} {check_file}")

    spark.stop()

    # Non-zero exit on any failure — Airflow will detect this on Day 7
    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
