#!/usr/bin/env bash
set -euo pipefail

echo "=== ETL Pipeline - Inside Airbnb ==="

echo "1. Extracting data..."
python extract_data.py

echo "2. Transforming data..."
python transform_data.py

echo "3. Loading data to GCS..."
python load_gcp.py

echo "4. Ingesting to Hive..."
bash ingest_to_hive.sh

echo "5. Running analyses..."
bash run_analyses.sh

echo "=== Pipeline completed successfully ==="