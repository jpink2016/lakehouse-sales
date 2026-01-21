
# Databricks notebook source
from datetime import datetime, date,timedelta
import random
import uuid
from src.common.config import load_config
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window
import argparse
from src.common.config import load_config

p = argparse.ArgumentParser()
p.add_argument("--env", required=True)
p.add_argument("--storage_root", required=True)
p.add_argument("--process_date", required=True)
a = p.parse_args()

cfg = load_config(env=a.env, storage_root=a.storage_root, process_date=a.process_date)
storage_root = cfg.storage_root
process_date = cfg.process_date
orders_out = cfg.paths["raw_orders_daily"]
items_out  = cfg.paths["raw_order_items_daily"]
meta_out   = f"{storage_root}/raw/_generator_runs"

print("env:", cfg.env)
print("storage_root:", cfg.storage_root)
print("process_date:", cfg.process_date)
print("orders_out:", cfg.paths["raw_orders_daily"])
# Generator-only parameters (safe locally & in Databricks)
def _get(name, default):
    try:
        return dbutils.widgets.get(name)  # type: ignore[name-defined]
    except Exception:
        return default

new_orders  = int(_get("new_orders", 200))
update_rate = float(_get("update_rate", 0))
late_rate   = float(_get("late_rate", 0.03))
dup_rate    = float(_get("dup_rate", 0.02))

if not storage_root:
    raise ValueError("storage_root is required (s3://... or abfss://...)")
if not process_date:
    raise ValueError("process_date is required (YYYY-MM-DD)")

run_id = str(uuid.uuid4())
run_ts = datetime.utcnow().isoformat()

# -----------------------------
# Dimension seeds (small, stable)
# You can later replace these with real customers/products.
# -----------------------------
random.seed(42)  # deterministic-ish across runs for sanity

countries = ["US", "CA", "GB", "DE", "FR", "AU"]
product_catalog = [
    ("P001", "Widget", "Gadgets", 19.99),
    ("P002", "Gizmo", "Gadgets", 29.99),
    ("P003", "Sprocket", "Parts", 5.49),
    ("P004", "Doodad", "Accessories", 12.00),
    ("P005", "Thingamajig", "Accessories", 9.75),
]

# We'll keep a simple customer pool
customer_ids = [f"C{str(i).zfill(5)}" for i in range(1, 2001)]  # 2k customers

# -----------------------------
# Helper: create random timestamps within the process_date
# -----------------------------
def rand_time_in_day(date_str: str):
    # returns ISO timestamp string
    hh = random.randint(0, 22)
    mm = random.randint(0, 59)
    ss = random.randint(0, 59)
    return f"{date_str} {hh:02d}:{mm:02d}:{ss:02d}"

# We'll generate on the driver, then parallelize. For small daily volumes this is fine.
orders_rows = []
items_rows = []

# New orders today
for i in range(new_orders):
    order_id = f"O{process_date.replace('-', '')}-{uuid.uuid4().hex[:10]}"
    customer_id = random.choice(customer_ids)
    status = "CREATED"

    order_ts = rand_time_in_day(process_date)

    # Late-arriving: a small fraction have order_ts from 1–3 days earlier
    if random.random() < late_rate:
        days_back = random.randint(1, 3)
        # crude: just decrement day number in string; for real date math use python datetime
        # but keep it simple: use Spark later for robust date logic if desired
        # We'll do proper date math here:
        dt = datetime.strptime(process_date, "%Y-%m-%d").date()
        older = dt.fromordinal(dt.toordinal() - days_back).isoformat()
        order_ts = rand_time_in_day(older)
    
    # convert string → datetime
    order_dt = datetime.strptime(order_ts, "%Y-%m-%d %H:%M:%S")
    updated_ts = order_dt + timedelta(seconds=random.randint(0, 600))  # 0–10 min

    orders_rows.append((order_id, customer_id, order_ts, status, updated_ts.isoformat(" ")))

    # items: 1–5 per order
    n_items = random.randint(1, 5)
    for j in range(n_items):
        prod = random.choice(product_catalog)
        product_id, _, _, unit_price = prod
        qty = random.randint(1, 4)
        order_item_id = f"OI-{uuid.uuid4().hex[:12]}"
        items_rows.append((order_item_id, order_id, product_id, qty, float(unit_price)))

# -----------------------------
# Optionally: generate "updates" to prior orders to simulate CDC
# Strategy: read some recent order_ids from previous raw partitions if they exist,
# then emit updated versions today (status changes / updated_ts bumps).
# -----------------------------
# We'll try reading from silver/orders later in your pipeline; but for now we can read
# previous raw orders if present. If nothing exists yet, this just skips.

from pyspark.errors.exceptions.captured import AnalysisException
def lookback_paths(root: str, process_date: str, days_back_start=2, days_back_end=10):
    # returns list like [ingest_date=n-1, n-2, n-3]
    dt = datetime.strptime(process_date, "%Y-%m-%d").date()
    dates = [(dt - timedelta(days=d)).isoformat() for d in range(days_back_start, days_back_end + 1)]
    base = f"{root.rstrip('/')}/raw/orders"
    return [f"{base}/ingest_date={d}/" for d in dates]
    
def try_read_recent_orders(root: str, process_date: str):
    paths = lookback_paths(root, process_date, days_back_start=1, days_back_end=3)
    try:
        df = (spark.read.json(paths)
              .select("order_id", "customer_id", "order_ts", "updated_ts", "status")
              .withColumn("order_ts", F.to_timestamp("order_ts"))
              .withColumn("updated_ts", F.to_timestamp("updated_ts"))
        )
        # skip terminal states
        df = df.filter(~F.col("status").isin("CANCELLED", "SHIPPED"))
        return df
    except AnalysisException as e:
        # if none of the lookback paths exist yet, treat as first run
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            return None
        raise
    except Exception:
        return None
    
def next_status(status: str):
    # terminal stays terminal
    if status in ("SHIPPED", "CANCELLED"):
        return status
    statuses = ["CREATED", "PAID", "SHIPPED"]
    if status not in statuses:
        return "CREATED"
    i = statuses.index(status)
    return statuses[min(i + 1, len(statuses) - 1)]  # stop at SHIPPED

recent_orders_df = None
if update_rate > 0:
    recent_orders_df = try_read_recent_orders(storage_root, process_date)
    if recent_orders_df is not None:
        w = Window.partitionBy("order_id").orderBy(F.col("updated_ts").desc_nulls_last(), F.col("order_ts").desc_nulls_last())
        recent_orders_df = (recent_orders_df
            .withColumn("_rn", F.row_number().over(w))
            .filter("_rn = 1")
            .drop("_rn")
        )

update_rows = []
if recent_orders_df is not None:
    total_recent = recent_orders_df.select("order_id").distinct().count()
    if total_recent > 0:
        n_updates = int(total_recent * update_rate)

        sampled = (recent_orders_df
                   .orderBy(F.rand())
                   .limit(n_updates)
                   .collect())

        for r in sampled:
            oid = r["order_id"]
            customer_id = r["customer_id"] or random.choice(customer_ids)
            prev_order_ts = r["order_ts"]
            prev_updated_ts = r["updated_ts"] or prev_order_ts

            cand = datetime.strptime(rand_time_in_day(process_date), "%Y-%m-%d %H:%M:%S")
            base = max(prev_updated_ts or prev_order_ts, prev_order_ts)

            if cand <= base:
                cand = base + timedelta(minutes=random.randint(5, 59))

            updated_ts = cand
            order_ts = prev_order_ts

            status = next_status(r["status"])

            update_rows.append((
                oid,
                customer_id,
                order_ts,
                status,
                updated_ts,
            ))

orders_rows.extend(update_rows)

# -----------------------------
# Inject duplicates (exact dup rows) to test dedupe logic
# -----------------------------
def inject_dups(rows, rate):
    if not rows:
        return rows
    k = max(1, int(len(rows) * rate))
    dup_samples = random.sample(rows, min(k, len(rows)))
    return rows + dup_samples

orders_rows = inject_dups(orders_rows, dup_rate)
items_rows = inject_dups(items_rows, dup_rate)

# -----------------------------
# Build Spark DataFrames
# -----------------------------
orders_schema = T.StructType([
    T.StructField("order_id", T.StringType(), False),
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("order_ts", T.StringType(), False),
    T.StructField("status", T.StringType(), False),
    T.StructField("updated_ts", T.StringType(), False),
])

items_schema = T.StructType([
    T.StructField("order_item_id", T.StringType(), False),
    T.StructField("order_id", T.StringType(), False),
    T.StructField("product_id", T.StringType(), False),
    T.StructField("qty", T.IntegerType(), False),
    T.StructField("unit_price", T.DoubleType(), False),
])

orders_df = spark.createDataFrame(orders_rows, orders_schema) \
    .withColumn("ingest_date", F.lit(process_date)) \
    .withColumn("run_id", F.lit(run_id)) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("synthetic_generator")) \
    .withColumn("order_ts", F.to_timestamp("order_ts")) \
    .withColumn("updated_ts", F.to_timestamp("updated_ts"))

items_df = spark.createDataFrame(items_rows, items_schema) \
    .withColumn("ingest_date", F.lit(process_date)) \
    .withColumn("run_id", F.lit(run_id)) \
    .withColumn("ingested_at", F.current_timestamp()) \
    .withColumn("source_system", F.lit("synthetic_generator"))

# -----------------------------
# Write as raw JSON files into raw landing folders
# (This mimics "file drops" you'd ingest later.)
# -----------------------------
(orders_df
    .repartition(1)
    .write.mode("overwrite")
    .json(orders_out))

(items_df
    .repartition(1)
    .write.mode("overwrite")
    .json(items_out))

# Optional: record generator run metadata
meta = spark.createDataFrame(
    [(run_id, process_date, run_ts, new_orders, len(update_rows), late_rate, dup_rate)],
    "run_id string, process_date string, run_ts string, new_orders int, num_updates int, late_rate double, dup_rate double"
)
(meta.write.format("delta").mode("append").save(meta_out))

display(orders_df.limit(10))
display(items_df.limit(10))
print(f"Wrote orders to: {orders_out}")
print(f"Wrote items  to: {items_out}")
print(f"Run metadata:  {meta_out}")
