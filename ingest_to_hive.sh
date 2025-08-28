#!/usr/bin/env bash
set -euo pipefail

REGION="us-central1"
CLUSTER="airbnb-hive-clusterx"

echo "Creating database..."
gcloud dataproc jobs submit hive \
  --cluster "$CLUSTER" \
  --region "$REGION" \
  --file hive/create_database.hql

echo "Creating tables..."
gcloud dataproc jobs submit hive \
  --cluster "$CLUSTER" \
  --region "$REGION" \
  --file hive/create_tables.hql

echo "Hive tables created successfully."