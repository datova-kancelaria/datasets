# egov

Harvests public MetaIS data into a local output directory.

This module has two parts:

1. `fetch-reports.sh` downloads public MetaIS report exports.
2. `cloud_services.py` fetches `AS` and `InfraSluzba` entities from the public CMDB `cilistfiltered` endpoint and builds cloud-service outputs.

No secrets are required.

## Files

- `fetch-reports.sh` — downloads public report JSON files
- `convert.py` — converts report JSON files into CSV and rewrites the JSON into a normalized shape
- `cloud_services.py` — public CMDB fetch + metadata merge + Excel/JSON output
- `helper.py` — HTTP helpers, metadata parsing, enum lookup, Excel writing, CMDB window splitting
- `sync_params_AS_IS.json` — merge schema used when harmonizing `AS` and `InfraSluzba`

## Public report fetch

`fetch-reports.sh` downloads these report IDs from the public MetaIS report endpoint:

- `43101` → `KS.json`
- `43102` → `AS.json`
- `43103` → `ISVS.json`
- `43104` → `Projekt.json`
- `43105` → `InfraSluzba.json`
- `43106` → `KRIS.json`

### Arguments

```bash
./egov/fetch-reports.sh --out-dir /path/to/egov
```

If `--out-dir` is omitted, the script falls back to:

1. `EGOV_OUT_DIR` if set and non-empty
2. `<repo>/data/egov`

### Optional environment variables

- `CURL_RETRIES`
- `CURL_RETRY_DELAY`
- `CURL_CONNECT_TIMEOUT`
- `CURL_MAX_TIME`

## Report conversion

`convert.py` expects the report JSON files above, writes CSV versions, and rewrites each JSON into a simplified form with:

- `header`
- `rows`

### Arguments

```bash
python egov/convert.py --data-dir /path/to/egov
```

If `--data-dir` is omitted, the default is `<repo>/data/egov`.

## Cloud services pipeline

`cloud_services.py` no longer depends on a private report URL or GitHub secret.

It fetches `AS` and `InfraSluzba` through the public `cilistfiltered` endpoint, using `createdAtFrom` / `createdAtTo` windows. Large windows are recursively split until the probe count is strictly below the configured threshold, then each accepted window is paged and fetched.

### Arguments

```bash
python egov/cloud_services.py --out-dir /path/to/egov
```

Optional tuning flags:

```bash
python egov/cloud_services.py \
  --out-dir /path/to/egov \
  --created-at-from 2000-01-01T00:00:00.000 \
  --created-at-to 2026-04-04T00:00:00.000 \
  --window-target-count 9500 \
  --page-size 1000 \
  --probe-page-size 1
```

### What it does

1. fetch `AS` and `InfraSluzba` from public CMDB using split windows
2. fetch attribute metadata for both entity types
3. fetch required enum values
4. sanitize and normalize the raw entities
5. merge attribute metadata according to `sync_params_AS_IS.json`
6. harmonize both entity sets into one shared schema
7. write combined Excel and JSON outputs

### Outputs

In `raw/`:

- `AS.json`
- `InfraSluzba.json`
- `AS_meta.json`
- `InfraSluzba_meta.json`
- `AS_fetch_windows.json`
- `InfraSluzba_fetch_windows.json`
- `AS_IS_merged_meta.json`
- `AS_harmonized.json`
- `InfraSluzba_harmonized.json`
- `AS_IS_combined.json`

In the main output directory:

- `CloudSluzba.xlsx`
- `CloudSluzba_curated.xlsx`
- `CloudSluzba.json`

## Top-level orchestration

The repo-level orchestrator calls this module as:

```bash
./egov/fetch-reports.sh --out-dir "$EGOV_OUT_DIR"
python egov/convert.py --data-dir "$EGOV_OUT_DIR"
python egov/cloud_services.py --out-dir "$EGOV_OUT_DIR"
```

Only `--out-dir` / `--data-dir` are passed by the top-level script. The optional `cloud_services.py` tuning flags are available for manual runs.
