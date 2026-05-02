"""
dags/contract_validation_dag.py
---------------------------------
Lightweight Airflow DAG: runs Soda contract checks across ALL layers
without running the full pipeline. Useful for:
  - Monitoring data quality between main pipeline runs
  - Detecting stale data via freshness checks
  - Running after manual Bronze writes or backfills

Schedule: every hour
"""

import os
import logging
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

REPO_ROOT   = Path(__file__).parent.parent
SODA_RUNNER = REPO_ROOT / "observability" / "soda_checks" / "run_soda_checks.py"

default_args = {
    "owner":            "arcofiero",
    "depends_on_past":  False,
    "email_on_failure": False,
    "retries":          0,
}

with DAG(
    dag_id="contract_validation",
    description="Hourly Soda Core contract checks across Bronze, Silver, and Gold layers",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["soda", "contracts", "quality", "bronze", "silver", "gold"],
) as dag:

    check_bronze = BashOperator(
        task_id="check_bronze_contracts",
        bash_command=f"""
            echo "=== Bronze contract checks ==="
            cd {REPO_ROOT}
            python3 {SODA_RUNNER} --layer bronze
        """,
        env={**os.environ},
    )

    check_silver = BashOperator(
        task_id="check_silver_contracts",
        bash_command=f"""
            echo "=== Silver contract checks ==="
            cd {REPO_ROOT}
            python3 {SODA_RUNNER} --layer silver
        """,
        env={**os.environ},
    )

    check_gold = BashOperator(
        task_id="check_gold_contracts",
        bash_command=f"""
            echo "=== Gold contract checks ==="
            cd {REPO_ROOT}
            python3 {SODA_RUNNER} --layer gold
        """,
        env={**os.environ},
    )

    def contract_summary(**context):
        dag_run = context["dag_run"]
        task_states = {}
        for task_id in ["check_bronze_contracts", "check_silver_contracts", "check_gold_contracts"]:
            ti = dag_run.get_task_instance(task_id)
            task_states[task_id] = ti.state if ti else "unknown"

        failed = [t for t, s in task_states.items() if s == "failed"]
        logging.info("=" * 50)
        logging.info("  CONTRACT VALIDATION SUMMARY")
        logging.info(f"  Run: {dag_run.run_id}")
        logging.info("=" * 50)
        for task_id, state in task_states.items():
            icon = "OK" if state == "success" else "FAIL" if state == "failed" else "SKIP"
            logging.info(f"  [{icon}] {task_id}: {state}")
        if failed:
            logging.warning(f"Contract violations detected: {failed}")
        else:
            logging.info("All contract checks passed.")
        logging.info("=" * 50)

    summary = PythonOperator(
        task_id="contract_summary",
        python_callable=contract_summary,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    check_bronze >> check_silver >> check_gold >> summary
