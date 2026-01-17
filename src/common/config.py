# src/common/config.py
from __future__ import annotations

import os
import argparse
from dataclasses import dataclass
from typing import Dict, Optional


def _get_widget(name: str) -> Optional[str]:
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return None


def _first(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return None


@dataclass(frozen=True)
class Config:
    env: str
    storage_root: str
    process_date: str
    paths: Dict[str, str]


def build_paths(storage_root: str, process_date: str) -> Dict[str, str]:
    root = storage_root.rstrip("/")

    bronze = f"{root}/bronze"
    silver = f"{root}/silver"
    gold   = f"{root}/gold"

    return {
        "bronze_orders_daily": f"{bronze}/orders/ingest_date={process_date}",
        "bronze_order_items_daily": f"{bronze}/order_items/ingest_date={process_date}",

        "silver_orders": f"{silver}/orders",
        "silver_order_items": f"{silver}/order_items",

        "gold_daily_revenue": f"{gold}/daily_revenue",

        "dq_results": f"{root}/quality/dq_results",
    }


def load_config() -> Config:
    # 1️⃣ CLI args (Spark Python task)
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument("--env")
    parser.add_argument("--storage_root")
    parser.add_argument("--process_date")
    args, _ = parser.parse_known_args()

    # 2️⃣ Widgets (only if running as notebook)
    w_env   = _get_widget("env")
    w_root  = _get_widget("storage_root")
    w_date  = _get_widget("process_date")

    # 3️⃣ Environment variables (local / Docker)
    e_env  = os.getenv("ENV")
    e_root = os.getenv("STORAGE_ROOT")
    e_date = os.getenv("PROCESS_DATE")

    env = (_first(args.env, w_env, e_env, "dev") or "dev").lower()
    storage_root = _first(args.storage_root, w_root, e_root)
    process_date = _first(args.process_date, w_date, e_date)

    if not storage_root:
        raise ValueError("Missing storage_root (use --storage_root or STORAGE_ROOT).")
    if not process_date:
        raise ValueError("Missing process_date (use --process_date or PROCESS_DATE).")

    return Config(
        env=env,
        storage_root=storage_root,
        process_date=process_date,
        paths=build_paths(storage_root, process_date),
    )
