"""
flink/bronze_writer.py
-----------------------
Writes decoded Kafka events to the Delta Lake Bronze layer on AWS S3.

Design:
  - PySpark + delta-spark for Delta compatibility
  - Micro-batch write pattern: accumulates records, then flushes on threshold
  - Appends ingestion metadata to every row
  - DLQ rows written to bronze/dlq Delta table
  - Partitioned by event_date
  - Delta transaction log prevents duplicate rows on retry
"""

import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from delta.pip_utils import configure_spark_with_delta_pip

from config.flink_config import (
    DELTA_BASE_PATH,
    DELTA_WRITE_MODE,
    CHECKPOINT_BASE,
    BATCH_MAX_RECORDS,
    BATCH_TIMEOUT_SEC,
    get_spark_conf,
)
from bronze.bronze_schema import BRONZE_TABLE_REGISTRY

logger = logging.getLogger(__name__)


def build_spark_session() -> SparkSession:
    """Build a PySpark session configured for Delta Lake + S3A."""
    conf = get_spark_conf()
    builder = SparkSession.builder
    for k, v in conf.items():
        builder = builder.config(k, v)
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        f"SparkSession created: app={spark.conf.get('spark.app.name')} "
        f"master={spark.conf.get('spark.master')}"
    )
    return spark


class BronzeWriter:
    """
    Accumulates decoded Kafka records into per-topic buffers.
    Flushes to Delta Lake Bronze tables when:
      - Buffer hits BATCH_MAX_RECORDS, OR
      - BATCH_TIMEOUT_SEC seconds have elapsed since last flush.
    """

    def __init__(self, spark: SparkSession):
        self._spark = spark
        self._buffers: dict[str, list[dict]] = {
            topic: [] for topic in BRONZE_TABLE_REGISTRY
        }
        self._last_flush_ts: float = self._now_ts()
        self._stats: dict[str, int] = {t: 0 for t in BRONZE_TABLE_REGISTRY}

    def add(self, topic_key: str, row: dict) -> None:
        if topic_key not in self._buffers:
            logger.error(f"Unknown topic key '{topic_key}' -- skipping row")
            return
        self._buffers[topic_key].append(row)

    def add_dlq_rows(self, dlq_rows: list[dict]) -> None:
        for row in dlq_rows:
            self.add("dlq", row)

    def should_flush(self) -> bool:
        total_pending = sum(len(buf) for buf in self._buffers.values())
        elapsed = self._now_ts() - self._last_flush_ts
        return total_pending >= BATCH_MAX_RECORDS or elapsed >= BATCH_TIMEOUT_SEC

    def flush(self) -> dict[str, int]:
        """Write all non-empty buffers to their Delta Bronze tables."""
        flush_counts: dict[str, int] = {}
        now_utc = datetime.now(timezone.utc)

        for topic_key, rows in self._buffers.items():
            if not rows:
                continue

            registry_entry = BRONZE_TABLE_REGISTRY[topic_key]
            schema         = registry_entry["schema"]
            delta_path     = registry_entry["delta_path"]
            partition_cols = registry_entry["partition_cols"]
            full_path      = f"{DELTA_BASE_PATH}/{delta_path}"

            try:
                df = self._build_dataframe(rows, schema, now_utc)
                self._write_delta(df, full_path, partition_cols, topic_key)
                written = len(rows)
                flush_counts[topic_key] = written
                self._stats[topic_key] += written
                logger.info(
                    f"[Bronze flush] topic={topic_key} rows={written} path={full_path}"
                )
            except Exception as e:
                logger.error(
                    f"[Bronze flush] FAILED for topic={topic_key} "
                    f"rows={len(rows)} error={e}",
                    exc_info=True,
                )
                continue  # Don't clear buffer on failure -- allows retry

            self._buffers[topic_key].clear()

        self._last_flush_ts = self._now_ts()
        return flush_counts

    def flush_and_close(self) -> None:
        logger.info("BronzeWriter shutting down -- flushing remaining buffers...")
        self.flush()
        logger.info(f"BronzeWriter closed. Lifetime write stats: {self._stats}")

    @property
    def stats(self) -> dict[str, int]:
        return dict(self._stats)

    def _build_dataframe(
        self,
        rows: list[dict],
        schema: StructType,
        now_utc: datetime,
    ) -> DataFrame:
        ingested_at_str = now_utc.isoformat()
        enriched = []
        for row in rows:
            r = dict(row)
            if "_ingested_at" not in r or r["_ingested_at"] is None:
                r["_ingested_at"] = ingested_at_str
            if "event_date" not in r or r["event_date"] is None:
                r["event_date"] = now_utc.strftime("%Y-%m-%d")
            enriched.append(r)

        df = self._spark.createDataFrame(enriched, schema=schema)

        if "_ingested_at" in [f.name for f in schema.fields]:
            df = df.withColumn("_ingested_at", F.to_timestamp(F.col("_ingested_at")))
        return df

    def _write_delta(
        self,
        df: DataFrame,
        full_path: str,
        partition_cols: list[str],
        topic_key: str,
    ) -> None:
        (
            df.write
            .format("delta")
            .mode(DELTA_WRITE_MODE)
            .partitionBy(*partition_cols)
            .option("mergeSchema", "true")
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/bronze/{topic_key}")
            .save(full_path)
        )

    @staticmethod
    def _now_ts() -> float:
        return datetime.now(timezone.utc).timestamp()
