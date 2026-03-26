#!/usr/bin/env bash
# Copy this file to a local machine-only path such as:
#   /opt/datasets/runner-env.sh
# Then replace the placeholder values with real ones.
#
# This file is meant to be sourced:
#   source /opt/datasets/runner-env.sh

# Python interpreter used by the pipeline
export PYTHON_BIN=/absolute/path/to/venv/bin/python

# Directory containing CES credential files:
#   APIKEY
#   USER
#   PASS
export CES_SECRETS_DIR=/absolute/path/to/ces-secrets

# Stable organization name used by the CES pipeline
export CES_ORG_NAME='your organization name here'

# Repository checkout root on the self-hosted runner
export REPO_ROOT=/absolute/path/to/repo

# Persistent output/cache location
export DATA_ROOT="$REPO_ROOT/data"

# Output subdirectories
export EGOV_OUT_DIR="$DATA_ROOT/egov"
export LOCATION_OUT_DIR="$DATA_ROOT/location"
export FINANCE_OUT_DIR="$DATA_ROOT/finance-mirri"

# Refresh threshold (days) for location/NUTS data
export LOCATION_DATA_DAYS_REFRESH=30

# CES harvest config file inside the repo
export CES_CONFIG="$REPO_ROOT/ces-harvest/config/datasets.json"

# Network retry/timeout tuning for curl-based fetches
export CURL_RETRIES=5
export CURL_RETRY_DELAY=2
export CURL_CONNECT_TIMEOUT=15
export CURL_MAX_TIME=180

# Optional proxy configuration
# Uncomment and fill in if your environment requires an HTTP/HTTPS proxy.
# export http_proxy=http://proxy-host:3128
# export https_proxy=http://proxy-host:3128
# export HTTP_PROXY=http://proxy-host:3128
# export HTTPS_PROXY=http://proxy-host:3128
# export NO_PROXY=localhost,127.0.0.1