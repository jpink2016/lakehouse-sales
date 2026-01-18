# src/common/config.py
from dataclasses import dataclass
from typing import Dict, Optional

@dataclass(frozen=True)
class Config:
    env: str
    storage_root: str
    process_date: str
    paths: Dict[str, str]

def build_paths(storage_root: str, process_date: str) -> Dict[str, str]:
    root = storage_root.rstrip("/")
    raw    = f"{root}/raw"
    bronze = f"{root}/bronze"
    silver = f"{root}/silver"
    gold   = f"{root}/gold"
    return {
        "raw_orders_daily": f"{raw}/orders/ingest_date={process_date}",
        "raw_order_items_daily": f"{raw}/order_items/ingest_date={process_date}",
        "bronze_orders": f"{bronze}/orders",
        "bronze_order_items": f"{bronze}/order_items",
        "silver_orders": f"{silver}/orders",
        "silver_order_items": f"{silver}/order_items",
        "gold_daily_revenue": f"{gold}/daily_revenue",
        "dq_results": f"{root}/quality/dq_results",
    }

def load_config(*, env: str, storage_root: str, process_date: str) -> Config:
    if not storage_root:
        raise ValueError("Missing storage_root")
    if not process_date:
        raise ValueError("Missing process_date")
    return Config(
        env=env.lower(),
        storage_root=storage_root,
        process_date=process_date,
        paths=build_paths(storage_root, process_date),
    )
