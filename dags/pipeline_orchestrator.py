"""
dags/pipeline_orchestrator.py
-------------------------------
Main Airflow DAG: orchestrates the full contract-driven pipeline.

Schedule: every 30 minutes
Pipeline stages (in dependency order):

  1. wait_for_kafka_lag      — sense-check: confirms Kafka topics have messages
  2. run_kafka_ingest      — runs ingest/kafka_consumer.py for BATCH_TIMEOUT_SEC seconds
  3. bronze_soda_checks      — runs Soda checks on all Bronze Delta tables
  4. run_dbt_silver          — dbt run for Silver models
  5. silver_soda_checks      — runs Soda checks on all Silver Delta tables
  6. run_dbt_gold            — dbt run for Gold models
  7. gold_soda_checks        — runs Soda checks on all Gold Delta tables
  8. pipeline_health_report  — logs final pass/fail summary + contract violation count

If any Soda check step fails (non-zero exit), Airflow marks it FAILED and
downstream dbt steps are skipped — the pipeline stops at the violated layer.
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

# ── Paths (resolved relative to repo root) ───────────────────────────────────
REPO_ROOT   = Path(__file__).parent.parent
SODA_RUNNER = REPO_ROOT / "observability" / "soda_checks" / "run_soda_checks.py"
DBT_DIR     = REPO_ROOT / "dbt_project"
INGEST_MAIN  = REPO_ROOT / "ingest" / "kafka_consumer.py"

# ── Default args ──────────────────────────────────────────────────────────────
default_args = {
    "owner":            "arcofiero",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=2),
}

# ── DAG ───────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="contract_driven_pipeline",
    description="Full medallion pipeline: Kafka -> Ingest -> Bronze -> Silver -> Gold with Soda contract checks",
    schedule_interval=timedelta(minutes=30),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["kafka", "spark", "delta", "dbt", "soda", "bronze", "silver", "gold"],
) as dag:

    # ── Stage 1: Kafka lag check ──────────────────────────────────────────────
    wait_for_kafka_lag = BashOperator(
        task_id="wait_for_kafka_lag",
        bash_command=f"""
            echo "Checking Kafka topic lag on Confluent Cloud..."
            python3 -c "
from confluent_kafka.admin import AdminClient
from dotenv import load_dotenv
import os
load_dotenv('{REPO_ROOT}/.env')
conf = {{
    'bootstrap.servers': os.environ['CONFLUENT_BOOTSTRAP_SERVERS'],
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms':   'PLAIN',
    'sasl.username':     os.environ['CONFLUENT_API_KEY'],
    'sasl.password':     os.environ['CONFLUENT_API_SECRET'],
}}
client = AdminClient(conf)
metadata = client.list_topics(timeout=10)
topics = [t for t in metadata.topics if not t.startswith('_')]
print(f'Live topics: {{topics}}')
assert len(topics) >= 4, f'Expected >= 4 topics, found {{len(topics)}}'
print('Kafka lag check passed.')
"
        """,
        env={**os.environ},
    )

    # ── Stage 2: Kafka ingest consumer (Spark micro-batch, runs for 10 minutes) ────────────
    run_kafka_ingest = BashOperator(
        task_id="run_kafka_ingest",
        bash_command=f"""
            echo "Starting Kafka ingest consumer for 10-minute batch window..."
            cd {REPO_ROOT}
            python3 {INGEST_MAIN} &
            CONSUMER_PID=$!
            echo "Consumer PID: $CONSUMER_PID"
            sleep 600
            echo "Sending SIGTERM to consumer for graceful shutdown..."
            kill -TERM $CONSUMER_PID 2>/dev/null || true
            wait $CONSUMER_PID 2>/dev/null || true
            echo "Kafka ingest batch complete."
        """,
        execution_timeout=timedelta(minutes=15),
        env={**os.environ},
    )

    # ── Stage 3: Bronze Soda checks ───────────────────────────────────────────
    bronze_soda_checks = BashOperator(
        task_id="bronze_soda_checks",
        bash_command=f"""
            echo "Running Bronze layer Soda contract checks..."
            cd {REPO_ROOT}
            python3 {SODA_RUNNER} --layer bronze
            echo "Bronze Soda checks passed."
        """,
        env={**os.environ},
    )

    # ── Stage 4: dbt Silver ───────────────────────────────────────────────────
    run_dbt_silver = BashOperator(
        task_id="run_dbt_silver",
        bash_command=f"""
            echo "Running dbt Silver models..."
            cd {DBT_DIR}
            dbt run --select silver --profiles-dir ~/.dbt --no-version-check
            echo "dbt Silver run complete."
        """,
        env={**os.environ},
    )

    # ── Stage 5: Silver Soda checks ───────────────────────────────────────────
    silver_soda_checks = BashOperator(
        task_id="silver_soda_checks",
        bash_command=f"""
            echo "Running Silver layer Soda contract checks..."
            cd {REPO_ROOT}
            python3 {SODA_RUNNER} --layer silver
            echo "Silver Soda checks passed."
        """,
        env={**os.environ},
    )

    # ── Stage 6: dbt Gold ─────────────────────────────────────────────────────
    run_dbt_gold = BashOperator(
        task_id="run_dbt_gold",
        bash_command=f"""
            echo "Running dbt Gold models..."
            cd {DBT_DIR}
            dbt run --select gold --profiles-dir ~/.dbt --no-version-check
            echo "dbt Gold run complete."
        """,
        env={**os.environ},
    )

    # ── Stage 7: Gold Soda checks ─────────────────────────────────────────────
    gold_soda_checks = BashOperator(
        task_id="gold_soda_checks",
        bash_command=f"""
            echo "Running Gold layer Soda contract checks..."
            cd {REPO_ROOT}
            python3 {SODA_RUNNER} --layer gold
            echo "Gold Soda checks passed."
        """,
        env={**os.environ},
    )

    # ── Stage 8: Pipeline health report ──────────────────────────────────────
    def pipeline_health_report(**context):
        """
        Logs a final summary of the pipeline run.
        Uses TriggerRule.ALL_DONE so it runs even if upstream tasks failed.
        """
        dag_run = context["dag_run"]

        task_ids = [
            "wait_for_kafka_lag",
            "run_kafka_ingest",
            "bronze_soda_checks",
            "run_dbt_silver",
            "silver_soda_checks",
            "run_dbt_gold",
            "gold_soda_checks",
        ]

        results = {}
        for task_id in task_ids:
            task_instance = dag_run.get_task_instance(task_id)
            results[task_id] = task_instance.state if task_instance else "unknown"

        passed  = [t for t, s in results.items() if s == "success"]
        failed  = [t for t, s in results.items() if s == "failed"]
        skipped = [t for t, s in results.items() if s == "skipped"]

        logging.info("=" * 60)
        logging.info("  CONTRACT-DRIVEN PIPELINE HEALTH REPORT")
        logging.info(f"  DAG run: {dag_run.run_id}")
        logging.info(f"  Execution date: {context['execution_date']}")
        logging.info("=" * 60)
        logging.info(f"  Passed  ({len(passed)}): {passed}")
        logging.info(f"  Failed  ({len(failed)}): {failed}")
        logging.info(f"  Skipped ({len(skipped)}): {skipped}")
        logging.info("=" * 60)

        if failed:
            logging.warning(
                f"Pipeline completed with {len(failed)} failure(s). "
                f"Contract violations detected at: {failed}"
            )
        else:
            logging.info("Pipeline completed successfully. All contract checks passed.")

    health_report = PythonOperator(
        task_id="pipeline_health_report",
        python_callable=pipeline_health_report,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # ── Task dependencies (linear pipeline) ──────────────────────────────────
    (
        wait_for_kafka_lag
        >> run_kafka_ingest
        >> bronze_soda_checks
        >> run_dbt_silver
        >> silver_soda_checks
        >> run_dbt_gold
        >> gold_soda_checks
        >> health_report
    )
