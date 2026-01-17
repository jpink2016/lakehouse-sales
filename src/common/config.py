
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Dict, Optional

def _get_widget(name: str) -> Optional[str]:
    """
    Read a Databricks widget if running in Databricks; otherwise return None.
    """
    try:
        # dbutils is injected in Databricks notebooks/jobs
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return None


def _get_param(name: str, default: Optional[str] = None) -> Optional[str]:
    """
    Priority: Databricks widget -> environment variable -> default.
    """
    v = _get_widget(name)
    if v is not None and str(v).strip() != "":
        return str(v).strip()

    v = os.getenv(name.upper())
    if v is not None and str(v).strip() != "":
        return str(v).strip()

    return default


@dataclass(frozen=True)
class Config:
    env: str                 # "azure" or "aws" (or "dev")
    storage_root: str        # s3://... OR abfss://...
    process_date: str        # YYYY-MM-DD
    paths: Dict[str, str]    # convenience paths


def build_paths(storage_root: str, process_date: str) -> Dict[str, str]:
    root = storage_root.rstrip("/")

    # Landing folders (daily drops)
    bronze_base = f"{root}/bronze"
    silver_base = f"{root}/silver"
    gold_base = f"{root}/gold"

    return {
        # Base
        "bronze_base": bronze_base,
        "silver_base": silver_base,
        "gold_base": gold_base,

        # Daily landing folders (raw files)
        "bronze_orders_daily": f"{bronze_base}/orders/ingest_date={process_date}",
        "bronze_order_items_daily": f"{bronze_base}/order_items/ingest_date={process_date}",

        # Delta table locations (you can switch these to UC tables later)
        "silver_orders": f"{silver_base}/orders",
        "silver_order_items": f"{silver_base}/order_items",
        "silver_customers": f"{silver_base}/customers",
        "silver_products": f"{silver_base}/products",

        "gold_daily_revenue": f"{gold_base}/daily_revenue",
        "gold_top_products_daily": f"{gold_base}/top_products_daily",
        "gold_customer_ltv": f"{gold_base}/customer_ltv",

        # Quality results
        "dq_results": f"{root}/quality/dq_results",
    }


def load_config() -> Config:
    """
    Load config in both environments:
      - Databricks Jobs/Notebooks via widgets
      - Local runs via env vars (ENV, STORAGE_ROOT, PROCESS_DATE)
    Required:
      - storage_root
      - process_date (YYYY-MM-DD)
    """
    env = (_get_param("env", "dev") or "dev").lower()
    storage_root = _get_param("storage_root")
    process_date = _get_param("process_date")

    if not storage_root:
        raise ValueError(
            "Missing storage_root. Provide as Databricks widget 'storage_root' "
            "or env var STORAGE_ROOT (e.g., s3://bucket/lake OR abfss://container@acct.dfs.core.windows.net/lake)."
        )
    if not process_date:
        raise ValueError(
            "Missing process_date. Provide as Databricks widget 'process_date' "
            "or env var PROCESS_DATE (YYYY-MM-DD)."
        )

    paths = build_paths(storage_root, process_date)
    return Config(env=env, storage_root=storage_root, process_date=process_date, paths=paths)
