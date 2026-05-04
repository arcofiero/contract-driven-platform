"""
observability/dashboards/setup_superset_datasources.py
--------------------------------------------------------
Programmatically creates Superset datasets and dashboard via the REST API.

Connects to the Gold Delta tables through SQLite for dev,
or directly via SparkSQL/Trino for production.

Run AFTER superset is running:
    python observability/dashboards/setup_superset_datasources.py

Steps:
  1. Authenticate with Superset API
  2. Create database connection (SQLite with Gold tables for dev)
  3. Create 3 datasets (gold_daily_order_revenue, gold_payment_success_rate, gold_pipeline_health)
  4. Create 5 charts
  5. Assemble all charts into a single dashboard
"""

import json
import os
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"
DB_PATH  = os.path.join(os.path.dirname(__file__), "gold_data.db")


def get_token() -> str:
    resp = requests.post(f"{BASE_URL}/api/v1/security/login", json={
        "username": USERNAME,
        "password": PASSWORD,
        "provider": "db",
        "refresh": True,
    })
    resp.raise_for_status()
    return resp.json()["access_token"]


def api(method: str, path: str, token: str, **kwargs):
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    resp = getattr(requests, method)(f"{BASE_URL}{path}", headers=headers, **kwargs)
    if not resp.ok:
        print(f"  ERROR {resp.status_code}: {resp.text[:300]}")
    return resp


def setup(token: str):
    print("=== Setting up Superset datasources and dashboard ===\n")

    # ── 1. Create database connection ─────────────────────────────────────────
    print("1. Creating database connection...")
    r = api("post", "/api/v1/database/", token, json={
        "database_name": "contract_driven_platform_gold",
        "sqlalchemy_uri": f"sqlite:///{DB_PATH}",
        "expose_in_sqllab": True,
        "allow_run_async": False,
        "extra": json.dumps({"allows_virtual_table_creation": True}),
    })
    if r.ok:
        db_id = r.json()["id"]
        print(f"   Database created (id={db_id})")
    else:
        r2 = api("get", "/api/v1/database/?q=(filters:!((col:database_name,opr:eq,val:contract_driven_platform_gold)))", token)
        db_id = r2.json()["result"][0]["id"] if r2.ok and r2.json()["count"] > 0 else 1
        print(f"   Using existing database (id={db_id})")

    # ── 2. Create datasets ────────────────────────────────────────────────────
    print("\n2. Creating datasets...")
    datasets: dict[str, int | None] = {
        "gold_daily_order_revenue": None,
        "gold_payment_success_rate": None,
        "gold_pipeline_health": None,
    }
    for table_name in datasets:
        r = api("post", "/api/v1/dataset/", token, json={
            "database": db_id,
            "table_name": table_name,
            "schema": None,
        })
        if r.ok:
            datasets[table_name] = r.json()["id"]
            print(f"   Dataset: {table_name} (id={datasets[table_name]})")
        else:
            print(f"   Could not create {table_name} — may already exist")

    # ── 3. Create charts ─────────────────────────────────────────────────────
    print("\n3. Creating charts...")
    charts: dict[str, int] = {}

    chart_defs = [
        {
            "key": "revenue_line",
            "dataset_key": "gold_daily_order_revenue",
            "payload": {
                "slice_name": "Daily Gross Revenue by Currency",
                "viz_type": "echarts_timeseries_line",
                "params": {
                    "viz_type": "echarts_timeseries_line",
                    "x_axis": "event_date",
                    "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "gross_revenue"}, "aggregate": "SUM", "label": "Gross Revenue"}],
                    "groupby": ["currency_code"],
                    "rich_tooltip": True, "show_legend": True,
                    "x_axis_title": "Date", "y_axis_title": "Gross Revenue",
                    "color_scheme": "supersetColors",
                },
            },
        },
        {
            "key": "order_bar",
            "dataset_key": "gold_daily_order_revenue",
            "payload": {
                "slice_name": "Daily Order Count",
                "viz_type": "echarts_timeseries_bar",
                "params": {
                    "viz_type": "echarts_timeseries_bar",
                    "x_axis": "event_date",
                    "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_orders"}, "aggregate": "SUM", "label": "Orders"}],
                    "groupby": [],
                    "rich_tooltip": True, "color_scheme": "supersetColors",
                    "x_axis_title": "Date", "y_axis_title": "Order Count",
                },
            },
        },
        {
            "key": "payment_line",
            "dataset_key": "gold_payment_success_rate",
            "payload": {
                "slice_name": "Payment Success Rate by Method",
                "viz_type": "echarts_timeseries_line",
                "params": {
                    "viz_type": "echarts_timeseries_line",
                    "x_axis": "event_date",
                    "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "success_rate_pct"}, "aggregate": "AVG", "label": "Success Rate %"}],
                    "groupby": ["payment_method"],
                    "rich_tooltip": True, "show_legend": True,
                    "y_axis_bounds": [0, 100],
                    "x_axis_title": "Date", "y_axis_title": "Success Rate %",
                    "color_scheme": "supersetColors",
                },
            },
        },
        {
            "key": "violations_area",
            "dataset_key": "gold_pipeline_health",
            "payload": {
                "slice_name": "Contract Violations by Topic and Error Type",
                "viz_type": "echarts_area",
                "params": {
                    "viz_type": "echarts_area",
                    "x_axis": "event_date",
                    "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "violation_count"}, "aggregate": "SUM", "label": "Violations"}],
                    "groupby": ["source_topic"],
                    "rich_tooltip": True, "show_legend": True, "opacity": 0.7,
                    "x_axis_title": "Date", "y_axis_title": "Violation Count",
                    "color_scheme": "supersetColors",
                },
            },
        },
        {
            "key": "error_pie",
            "dataset_key": "gold_pipeline_health",
            "payload": {
                "slice_name": "DLQ Error Type Breakdown",
                "viz_type": "pie",
                "params": {
                    "viz_type": "pie",
                    "groupby": ["error_type"],
                    "metric": {"expressionType": "SIMPLE", "column": {"column_name": "violation_count"}, "aggregate": "SUM", "label": "Violations"},
                    "show_legend": True, "show_labels": True, "labels_outside": True,
                    "color_scheme": "supersetColors",
                },
            },
        },
    ]

    for chart_def in chart_defs:
        ds_id = datasets.get(chart_def["dataset_key"])
        if ds_id is None:
            continue
        payload = {
            **chart_def["payload"],
            "datasource_id": ds_id,
            "datasource_type": "table",
            "params": json.dumps(chart_def["payload"]["params"]),
        }
        r = api("post", "/api/v1/chart/", token, json=payload)
        if r.ok:
            charts[chart_def["key"]] = r.json()["id"]
            print(f"   Chart: {chart_def['payload']['slice_name']} (id={charts[chart_def['key']]})")

    # ── 4. Create dashboard ───────────────────────────────────────────────────
    print("\n4. Creating dashboard...")

    chart_ids = list(charts.values())
    position: dict = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-1", "ROW-2", "ROW-3"], "parents": ["ROOT_ID"]},
    }

    rows = [
        ("ROW-1", chart_ids[:2]),
        ("ROW-2", chart_ids[2:3]),
        ("ROW-3", chart_ids[3:5]),
    ]
    for row_id, row_chart_ids in rows:
        children = []
        cols = max(len(row_chart_ids), 1)
        for i, cid in enumerate(row_chart_ids):
            col_id = f"COLUMN-{row_id}-{i}"
            chart_elem_id = f"CHART-{cid}"
            position[col_id] = {
                "type": "COLUMN", "id": col_id,
                "children": [chart_elem_id],
                "parents": ["ROOT_ID", "GRID_ID", row_id],
                "meta": {"width": 12 // cols, "background": "BACKGROUND_TRANSPARENT"},
            }
            position[chart_elem_id] = {
                "type": "CHART", "id": chart_elem_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID", row_id, col_id],
                "meta": {"chartId": cid, "width": 12 // cols, "height": 400},
            }
            children.append(col_id)
        position[row_id] = {
            "type": "ROW", "id": row_id,
            "children": children,
            "parents": ["ROOT_ID", "GRID_ID"],
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    r = api("post", "/api/v1/dashboard/", token, json={
        "dashboard_title": "Contract-Driven Platform - Pipeline Health",
        "slug": "contract-driven-pipeline-health",
        "published": True,
        "position_json": json.dumps(position),
        "metadata": json.dumps({
            "color_scheme": "supersetColors",
            "refresh_frequency": 300,
            "timed_refresh_immune_slices": [],
        }),
        "owners": [],
    })
    if r.ok:
        dash_id = r.json()["id"]
        print(f"   Dashboard created (id={dash_id})")
        if chart_ids:
            api("put", f"/api/v1/dashboard/{dash_id}", token, json={"charts": chart_ids})
            print(f"   {len(chart_ids)} charts added to dashboard")
        print(f"\n=== Setup complete ===")
        print(f"Open: http://localhost:8088/superset/dashboard/{dash_id}/")
    else:
        print("   Dashboard creation failed — see error above")


if __name__ == "__main__":
    token = get_token()
    setup(token)
