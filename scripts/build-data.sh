#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

if [[ -z "${DATA_ROOT:-}" ]]; then
  DATA_ROOT="$REPO_ROOT/data"
fi
if [[ ${EGOV_OUT_DIR+x} != x ]]; then
  EGOV_OUT_DIR="$DATA_ROOT/egov"
fi
if [[ ${LOCATION_OUT_DIR+x} != x ]]; then
  LOCATION_OUT_DIR="$DATA_ROOT/location"
fi
if [[ ${FINANCE_OUT_DIR+x} != x ]]; then
  FINANCE_OUT_DIR="$DATA_ROOT/finance-mirri"
fi
if [[ -z "${LOCATION_DATA_DAYS_REFRESH:-}" ]]; then
  LOCATION_DATA_DAYS_REFRESH=30
fi
if [[ -z "${PYTHON_BIN:-}" ]]; then
  PYTHON_BIN=python3
fi

mkdir -p "$DATA_ROOT"

echo "DATA_ROOT=$DATA_ROOT"
echo "EGOV_OUT_DIR=$EGOV_OUT_DIR"
echo "LOCATION_OUT_DIR=$LOCATION_OUT_DIR"
echo "FINANCE_OUT_DIR=$FINANCE_OUT_DIR"
echo "LOCATION_DATA_DAYS_REFRESH=$LOCATION_DATA_DAYS_REFRESH"
echo "PYTHON_BIN=$PYTHON_BIN"

if [[ -n "$EGOV_OUT_DIR" ]]; then
  mkdir -p "$EGOV_OUT_DIR"
  echo "[egov] running -> $EGOV_OUT_DIR"
  "$REPO_ROOT/egov/fetch-reports.sh" --out-dir "$EGOV_OUT_DIR"
  "$PYTHON_BIN" "$REPO_ROOT/egov/convert.py" --data-dir "$EGOV_OUT_DIR"
  "$PYTHON_BIN" "$REPO_ROOT/egov/cloud_services.py" --out-dir "$EGOV_OUT_DIR"
else
  echo "[egov] skipped (output dir disabled)"
fi

if [[ -n "$LOCATION_OUT_DIR" ]]; then
  mkdir -p "$LOCATION_OUT_DIR"
  echo "[nuts] running -> $LOCATION_OUT_DIR"
  "$PYTHON_BIN" "$REPO_ROOT/nuts/fetch-nuts.py" \
    --out-dir "$LOCATION_OUT_DIR" \
    --refresh-days "$LOCATION_DATA_DAYS_REFRESH"
else
  echo "[nuts] skipped (output dir disabled)"
fi

if [[ -n "$FINANCE_OUT_DIR" ]]; then
  mkdir -p "$FINANCE_OUT_DIR"
  echo "[ces-harvest] running -> $FINANCE_OUT_DIR"
  "$REPO_ROOT/ces-harvest/run.sh" --out-dir "$FINANCE_OUT_DIR"
else
  echo "[ces-harvest] skipped (output dir disabled)"
fi
