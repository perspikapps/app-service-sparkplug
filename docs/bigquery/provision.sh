#!/usr/bin/env bash
#
# Provisions the BigQuery dataset and tables described in
# docs/sparkplug-receiving-architecture.md (sections 6 and 7) for the Sparkplug B receiving side.
#
# Requires the `bq` CLI, authenticated against the target GCP project (e.g. `gcloud auth login`
# && `gcloud config set project <project>`, or run inside an environment that already has
# application-default credentials).
#
# Usage:
#   PROJECT_ID=my-gcp-project DATASET=sparkplug ./provision.sh
#
# Both PROJECT_ID and DATASET may also be passed as the first two positional arguments:
#   ./provision.sh my-gcp-project sparkplug
#
# Safe to re-run: `bq mk` is checked against existing datasets/tables first, so this script is
# idempotent rather than failing on a second run.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null && pwd)"
SCHEMA_DIR="$SCRIPT_DIR/schema"

PROJECT_ID="${1:-${PROJECT_ID:-}}"
DATASET="${2:-${DATASET:-sparkplug}}"

if [ -z "$PROJECT_ID" ]; then
  echo "PROJECT_ID is required (env var or first argument)." >&2
  exit 1
fi

if ! command -v bq >/dev/null 2>&1; then
  echo "The 'bq' CLI is required (part of the Google Cloud SDK)." >&2
  exit 1
fi

DATASET_REF="${PROJECT_ID}:${DATASET}"

echo "==> Ensuring dataset ${DATASET_REF} exists"
if ! bq show --dataset "$DATASET_REF" >/dev/null 2>&1; then
  bq mk --dataset "$DATASET_REF"
else
  echo "    already exists, skipping"
fi

create_table() {
  local table="$1"
  local schema_file="$2"
  shift 2
  local table_ref="${DATASET_REF}.${table}"

  if bq show --table "$table_ref" >/dev/null 2>&1; then
    echo "==> Table ${table_ref} already exists, skipping"
    return
  fi

  echo "==> Creating table ${table_ref}"
  bq mk --table "$@" "$table_ref" "$schema_file"
}

# Fact tables: partitioned by day on `timestamp`, clustered on the filter shape almost every
# query uses (device + metric within a time range).
create_table "sparkplug_metrics" "$SCHEMA_DIR/sparkplug_metrics.json" \
  --time_partitioning_field=timestamp \
  --time_partitioning_type=DAY \
  --clustering_fields=group_id,edge_node_id,device_id,metric_name

create_table "sparkplug_sessions" "$SCHEMA_DIR/sparkplug_sessions.json" \
  --time_partitioning_field=birth_time \
  --time_partitioning_type=DAY \
  --clustering_fields=group_id,edge_node_id

# Dimension tables: small and slowly-changing, no partitioning/clustering needed.
create_table "dim_metric" "$SCHEMA_DIR/dim_metric.json"
create_table "dim_asset" "$SCHEMA_DIR/dim_asset.json"

echo "==> Done. Dataset: ${DATASET_REF}"
