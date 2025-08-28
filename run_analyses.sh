#!/usr/bin/env bash
set -euo pipefail

REGION="us-central1"
CLUSTER="airbnb-hive-clusterx"

gcloud dataproc jobs submit hive \
  --cluster "$CLUSTER" \
  --region "$REGION" \
  --file hive/analyses.sql
