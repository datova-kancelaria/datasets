#!/usr/bin/env bash
# Copy this file to a local machine-only path such as:
#   /opt/datasets/runner-env.sh
# Then replace the placeholder values with real ones.
#
# This file is meant to be sourced:
#   source /opt/datasets/runner-env.sh
#
# Empty output-dir strings disable whole modules.
# No GitHub repository secrets are required by this pipeline.
# The CES module still needs local credential files when enabled.

# Python interpreter used by the pipeline.
# Leave empty to fall back to `python3` on PATH.
export PYTHON_BIN=/absolute/path/to/venv/bin/python

# Persistent output/cache location.
# Leave empty to use the repository default (`<repo>/data` when run locally,
# `$GITHUB_WORKSPACE/data` in GitHub Actions).
export DATA_ROOT=/absolute/path/to/persistent/data

# Output subdirectories. Leave empty to disable a module.
export EGOV_OUT_DIR=""
export LOCATION_OUT_DIR=""
export FINANCE_OUT_DIR="$DATA_ROOT/finance-<your-org>"

# Refresh threshold (days) for location/NUTS data.
export LOCATION_DATA_DAYS_REFRESH=30

# CES harvest config file.
# Leave empty to use the repository default:
#   <repo>/ces-harvest/config/datasets.json
export CES_CONFIG=

# Directory containing CES credential files.
# Required only when FINANCE_OUT_DIR is non-empty.
# Expected files:
#   APIKEY
#   USER
#   PASS
#   URI
export CES_SECRETS_DIR=/absolute/path/to/ces-secrets
# Example content of $CES_SECRETS_DIR/URI:
# {
#   "od001": "https://.../API_OD_001",
#   "od002": "https://.../API_OD_002",
#   "od003": "https://.../API_OD_003"
# }

# Stable organization name used by the CES pipeline.
# Required only when FINANCE_OUT_DIR is non-empty.
export CES_ORG_NAME='your organization name here'

# Optional override for the Unix user that runs CES through systemd-run.
# export CES_RUN_USER="$USER"

# Optional network retry/timeout tuning for curl-based eGov report fetches.
export CURL_RETRIES=5
export CURL_RETRY_DELAY=2
export CURL_CONNECT_TIMEOUT=15
export CURL_MAX_TIME=180

# Optional proxy configuration.
# Uncomment and fill in if your environment requires an HTTP/HTTPS proxy.
# export http_proxy=http://proxy-host:3128
# export https_proxy=http://proxy-host:3128
# export HTTP_PROXY=http://proxy-host:3128
# export HTTPS_PROXY=http://proxy-host:3128
# export NO_PROXY=localhost,127.0.0.1
