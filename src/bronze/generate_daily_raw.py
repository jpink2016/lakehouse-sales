
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("daily-raw-generator")
    .getOrCreate()
)
# Databricks notebook source
from datetime import datetime, date
import random
import uuid

from pyspark.sql import functions as F
from pyspark.sql import types as T

# -----------------------------
# Widgets (job parameters)
# -----------------------------
try: 
    dbutils.widgets.text('env','dev')
    dbutils.widgets.text("storage_root", "")
    dbutils.widgets.text("process_date", "")  # YYYY-MM-DD
    dbutils.widgets.text("new_orders", "200") # how many new orders to create today
    dbutils.widgets.text("update_rate", "0")  # fraction of prior orders to update
    dbutils.widgets.text("late_rate", "0.03")    # fraction of rows that arrive late (older order_ts)
    dbutils.widgets.text("dup_rate", "0.02")     # fraction of duplicates to inject
except Exception: 
    pass # local run

from src.common.config import load_config

cfg = load_config()
print(cfg.env, cfg.storage_root, cfg.process_date)
storage_root = cfg.storage_root
process_date = cfg.process_date
orders_out = cfg.paths["bronze_orders_daily"]
items_out  = cfg.paths["bronze_order_items_daily"]
meta_out   = f"{storage_root}/bronze/_generator_runs"

print("env:", cfg.env)
print("storage_root:", cfg.storage_root)
print("process_date:", cfg.process_date)
print("orders_out:", cfg.paths["bronze_orders_daily"])
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
    hh = random.randint(0, 23)
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

    updated_ts = rand_time_in_day(process_date)

    orders_rows.append((order_id, customer_id, order_ts, status, updated_ts))

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
# Strategy: read some recent order_ids from previous bronze partitions if they exist,
# then emit updated versions today (status changes / updated_ts bumps).
# -----------------------------
# We'll try reading from silver/orders later in your pipeline; but for now we can read
# previous bronze orders if present. If nothing exists yet, this just skips.

from pyspark.errors.exceptions.captured import AnalysisException

def try_read_recent_orders(root: str):
    path_glob = f"{root.rstrip('/')}/bronze/orders/ingest_date=*/"
    try:
        return spark.read.json(path_glob).select("order_id").distinct()
    except AnalysisException as e:
        # first run: nothing exists yet
        if "PATH_NOT_FOUND" in str(e) or "Path does not exist" in str(e):
            return None
        raise
    except Exception:
        # any other read issue: skip updates for now
        return None

recent_orders_df = None
if update_rate > 0:
    recent_orders_df = try_read_recent_orders(storage_root)

update_rows = []
if recent_orders_df is not None:
    # pick a sample to update
    total_recent = recent_orders_df.count()
    if total_recent > 0:
        n_updates = int(total_recent * update_rate)
        sampled = recent_orders_df.orderBy(F.rand()).limit(n_updates)
        sampled_ids = [r["order_id"] for r in sampled.collect()]

        for oid in sampled_ids:
            # simulate status progression
            status = random.choice(["PAID", "SHIPPED", "CANCELLED"])
            customer_id = random.choice(customer_ids)  # could keep same; simplified
            # updated_ts today
            updated_ts = rand_time_in_day(process_date)
            # keep original order_ts unknown here; set to today for bronze update event
            order_ts = rand_time_in_day(process_date)

            update_rows.append((oid, customer_id, order_ts, status, updated_ts))

# Append updates to today's orders payload (like CDC events)
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
# Write as raw JSON files into bronze landing folders
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
