#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

DATA_ROOT="${DATA_ROOT:-$REPO_ROOT/data}"
EGOV_OUT_DIR="${EGOV_OUT_DIR:-$DATA_ROOT/egov}"
LOCATION_OUT_DIR="${LOCATION_OUT_DIR:-$DATA_ROOT/location}"
FINANCE_OUT_DIR="${FINANCE_OUT_DIR:-$DATA_ROOT/finance-mirri}"
LOCATION_DATA_DAYS_REFRESH="${LOCATION_DATA_DAYS_REFRESH:-30}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$EGOV_OUT_DIR" "$LOCATION_OUT_DIR" "$FINANCE_OUT_DIR"

echo "DATA_ROOT=$DATA_ROOT"
echo "EGOV_OUT_DIR=$EGOV_OUT_DIR"
echo "LOCATION_OUT_DIR=$LOCATION_OUT_DIR"
echo "FINANCE_OUT_DIR=$FINANCE_OUT_DIR"
echo "LOCATION_DATA_DAYS_REFRESH=$LOCATION_DATA_DAYS_REFRESH"

"$REPO_ROOT/egov/fetch-reports.sh" --out-dir "$EGOV_OUT_DIR"
"$PYTHON_BIN" "$REPO_ROOT/egov/convert.py" --data-dir "$EGOV_OUT_DIR"
"$PYTHON_BIN" "$REPO_ROOT/egov/cloud_services.py" --out-dir "$EGOV_OUT_DIR"
"$PYTHON_BIN" "$REPO_ROOT/nuts/fetch-nuts.py" \
  --out-dir "$LOCATION_OUT_DIR" \
  --refresh-days "$LOCATION_DATA_DAYS_REFRESH"
"$REPO_ROOT/ces-harvest/run.sh" --out-dir "$FINANCE_OUT_DIR"
