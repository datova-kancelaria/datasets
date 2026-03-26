#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"

OUT_DIR=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --out-dir)
      OUT_DIR="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

OUT_DIR="${OUT_DIR:-${EGOV_OUT_DIR:-$REPO_ROOT/data/egov}}"

IDS=(43101 43102 43103 43104 43105 43106)
NAMES=(KS AS ISVS Projekt InfraSluzba KRIS)

BASE_URL="https://metais.slovensko.sk/api/report/reports/execute"
LANG="sk"

CURL_RETRIES="${CURL_RETRIES:-5}"
CURL_RETRY_DELAY="${CURL_RETRY_DELAY:-2}"
CURL_CONNECT_TIMEOUT="${CURL_CONNECT_TIMEOUT:-15}"
CURL_MAX_TIME="${CURL_MAX_TIME:-180}"

PAYLOAD='{
  "parameters": {
    "inclApplication": "true"
  }
}'

mkdir -p "$OUT_DIR"

fetch_report() {
  local report_id="$1"
  local out="$2"
  local url="${BASE_URL}/${report_id}/type/typ?lang=${LANG}"
  local tmp="${out}.tmp.$$"
  local attempt rc http_code sleep_for

  for ((attempt=1; attempt<=CURL_RETRIES; attempt++)); do
    echo "Fetching report ${report_id} -> ${out} (attempt ${attempt}/${CURL_RETRIES})"

    set +e
    http_code="$(
      curl \
        --silent \
        --show-error \
        --location \
        --connect-timeout "$CURL_CONNECT_TIMEOUT" \
        --max-time "$CURL_MAX_TIME" \
        --header 'Content-Type: application/json' \
        --data "$PAYLOAD" \
        --output "$tmp" \
        --write-out '%{http_code}' \
        "$url"
    )"
    rc=$?
    set -e

    if [[ $rc -eq 0 && "$http_code" =~ ^2 ]]; then
      mv "$tmp" "$out"
      return 0
    fi

    rm -f "$tmp"

    case "$http_code" in
      401|403|404)
        echo "Fatal HTTP ${http_code} for report ${report_id}" >&2
        return 1
        ;;
      408|429|500|502|503|504)
        ;;
      000)
        case "$rc" in
          6|7|28|35|52|55|56)
            ;;
          *)
            echo "Fatal curl error rc=${rc} for report ${report_id}" >&2
            return "$rc"
            ;;
        esac
        ;;
      *)
        echo "Fatal HTTP ${http_code} for report ${report_id}" >&2
        return 1
        ;;
    esac

    if (( attempt == CURL_RETRIES )); then
      echo "Giving up on report ${report_id} after ${CURL_RETRIES} attempts" >&2
      return 1
    fi

    sleep_for=$(( CURL_RETRY_DELAY * attempt ))
    echo "Transient failure for report ${report_id} (rc=${rc}, http=${http_code}), retrying in ${sleep_for}s..." >&2
    sleep "$sleep_for"
  done
}

for i in "${!IDS[@]}"; do
  id="${IDS[$i]}"
  filename="${NAMES[$i]}"
  out="$OUT_DIR/${filename}.json"
  fetch_report "$id" "$out"
done
