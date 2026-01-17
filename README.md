docker run --rm -it \
  -v "$PWD":/app \
  -w /app \
  -e PYTHONPATH=/app \
  -e STORAGE_ROOT=/app/_local_lake \
  -e PROCESS_DATE=2026-01-16 \
  apache/spark:4.1.1-r \
  /opt/spark/bin/spark-submit \
  --master "local[2]" \
  /app/src/bronze/ingest_daily.py

# ingest daily run with
cd /Users/jakepinkston/projects/lakehouse-sales
source .venv/bin/activate

export STORAGE_ROOT="/Users/jakepinkston/projects/lakehouse-sales/_local_lake"
export PROCESS_DATE="2026-01-16"
export ENV="aws"
python -m src.bronze.ingest_daily