from pyspark.sql import SparkSession
from src.common.config import load_config

spark = (
    SparkSession.builder
    .appName("daily-raw-generator")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

cfg = load_config()
storage_root = cfg.storage_root
process_date = cfg.process_date
orders_out = cfg.paths["bronze_orders_daily"]
items_out  = cfg.paths["bronze_order_items_daily"]
meta_out   = f"{storage_root}/bronze/_generator_runs"

def peek(df, name, n=10):
    print(f"\n=== {name} ===")
    print(f"rows: {df.count()}")
    df.limit(n).show(truncate=False)

df = spark.read.json(orders_out) 
peek(df, "orders_out")
