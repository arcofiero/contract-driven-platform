"""
observability/dashboards/superset_config.py
--------------------------------------------
Superset configuration for contract-driven-platform.

Usage:
    export SUPERSET_CONFIG_PATH=/Users/architraj/contract-driven-platform/observability/dashboards/superset_config.py
    superset run -p 8088 --with-threads --reload --debugger

Note: Superset requires Python <=3.12. Run in a venv with Python 3.11 or 3.12.
"""

import os
from dotenv import load_dotenv
load_dotenv()

# ── Core ──────────────────────────────────────────────────────────────────────
SECRET_KEY = os.getenv("SUPERSET_SECRET_KEY", "contract-driven-platform-superset-key")
SQLALCHEMY_DATABASE_URI = "sqlite:////Users/architraj/airflow/superset.db"

# ── Feature flags ─────────────────────────────────────────────────────────────
FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "DASHBOARD_NATIVE_FILTERS": True,
    "DASHBOARD_CROSS_FILTERS": True,
    "DASHBOARD_NATIVE_FILTERS_SET": True,
    "EMBEDDABLE_CHARTS": True,
}

# ── Cache ─────────────────────────────────────────────────────────────────────
CACHE_CONFIG = {
    "CACHE_TYPE": "SimpleCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
}

# ── Row limit ─────────────────────────────────────────────────────────────────
ROW_LIMIT = 50000
SUPERSET_WEBSERVER_PORT = 8088
ENABLE_PROXY_FIX = True
