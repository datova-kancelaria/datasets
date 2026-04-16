# nuts

Fetches public Slovak region / district / municipality / address data and writes CSV files.

The source catalog is:

- `https://rageo.minv.sk/opendata/katalog.json`

No secrets are required.

## Script

- `fetch-nuts.py` — downloads the dataset catalog, resolves dataset URIs, fetches GeoJSON-like payloads, and writes CSV outputs grouped by region

## Arguments

```bash
python nuts/fetch-nuts.py --out-dir /path/to/location
```

Optional refresh control:

```bash
python nuts/fetch-nuts.py --out-dir /path/to/location --refresh-days 30
```

If `--out-dir` is omitted, the default is `<repo>/data/location`.

If `--refresh-days` is omitted, the script uses:

1. `LOCATION_DATA_DAYS_REFRESH` from the environment if set
2. otherwise `30`

## Refresh logic

The script checks whether the expected outputs already exist and whether `kraje.csv` is newer than the configured refresh threshold.

If the data is still fresh, it skips the download.

## What it writes

In the output directory it writes:

- `kraje.csv`
- one CSV per region abbreviation:
  - `BSK.csv`
  - `TTSK.csv`
  - `TSK.csv`
  - `NSK.csv`
  - `ZSK.csv`
  - `BBSK.csv`
  - `PSK.csv`
  - `KSK.csv`
- `_UNKNOWN.csv` # optional
- `_PENDING_UNKNOWN.csv` # optional

## Processing flow

1. fetch the catalog JSON
2. locate dataset IRIs
3. fetch the underlying data payloads with retry logic
4. flatten features into CSV rows
5. learn region mappings from rows that already contain valid region information
6. try to fill missing region values from district / municipality identifiers and names
7. write per-region CSV outputs plus unknown/pending helper files

## Top-level orchestration

The repo-level orchestrator calls this module as:

```bash
python nuts/fetch-nuts.py \
  --out-dir "$LOCATION_OUT_DIR" \
  --refresh-days "$LOCATION_DATA_DAYS_REFRESH"
```
