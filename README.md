# datasets

Public data-fetching pipeline for three modules:

- `egov/` — MetaIS public report exports plus cloud service extraction from public CMDB endpoints
- `location/` — location / address datasets from `rageo.minv.sk`
- `ces-export/` — CES open-data fetcher that requires local CES credential files when enabled

The repository is designed to run either:

- locally from a clone, or
- in GitHub Actions on a self-hosted runner

Deployment-specific behavior is controlled by environment variables; dataset selection and scheduling live in JSON config files. You do **not** need to edit code in order to change output locations or disable modules.

## Module enable / disable

Each top-level module is controlled by its output directory variable:

- `EGOV_OUT_DIR`
- `LOCATION_OUT_DIR`
- `CES_EXPORT_OUT_DIR`

Behavior:

- variable set to a **non-empty** path → run the module there
- variable set to the **empty string** → skip the module
- variable **unset** → the current orchestrator treats it as disabled unless a runner env file sets it; for predictable deployment, set it explicitly in `runner-env.sh`

This skip-by-empty-dir behavior is implemented in both:

- `scripts/build-data.sh`
- `.github/workflows/datasets.yml`

So, for example, if `CES_EXPORT_OUT_DIR=""`, the CES module is skipped and the workflow does not require or validate `CES_ORG_NAME`, `CES_SECRETS_DIR`, or CES credential files.

## Secrets and credentials

There are **no GitHub Actions repository secrets** required by this repo.

The `egov` and `location` modules use public endpoints.

The `ces-export` module still requires local credential files when enabled:

- `APIKEY`
- `USER`
- `PASS`
- `URI`

These are provided from a machine-local directory via `CES_SECRETS_DIR` and passed into `systemd-run` by `ces-export/run.sh`.

## Repository layout

- `scripts/build-data.sh` — top-level orchestrator run by GitHub Actions and suitable for local runs
- `.github/workflows/datasets.yml` — self-hosted runner workflow
- `runner-env.example.sh` — example machine-local environment file
- `make_index.py` — builds directory index pages for the exported output tree
- `egov/` — MetaIS public report and CMDB cloud-service pipeline
- `location/` — location/address CSV fetching
- `ces-export/` — CES exporting package and wrapper script
- `resources/` — static assets used by generated directory indexes

## Setup

### 1. Clone the repo

```bash
git clone https://github.com/datova-kancelaria/datasets.git
cd datasets
```

### 2. Create the runner env file

Copy `runner-env.example.sh` to a machine-local path such as:

```bash
sudo mkdir -p /opt/datasets
sudo cp runner-env.example.sh /opt/datasets/runner-env.sh
sudo chmod 600 /opt/datasets/runner-env.sh
```

Then edit it.

### 3. Set the core environment values

Typical values:

```bash
export PYTHON_BIN=/absolute/path/to/venv/bin/python
export DATA_ROOT=/absolute/path/to/persistent/data

# MIRRI fetches these and publishes into https://datova-kancelaria.github.io/datasets/egov or /location, leave empty to disable
export EGOV_OUT_DIR=""
export LOCATION_OUT_DIR=""
# keep this non-empty to export CES data for your org
export CES_EXPORT_OUT_DIR="$DATA_ROOT/finance-<your-org>"

export LOCATION_DATA_DAYS_REFRESH=30
export CES_CONFIG=/opt/datasets/datasets.json
```

To disable a module, set its output directory to the empty string:

```bash
export CES_EXPORT_OUT_DIR=""
```

### 4. CES-only settings when ces-export is enabled

Only needed when `CES_EXPORT_OUT_DIR` is non-empty:

```bash
export CES_SECRETS_DIR=/absolute/path/to/ces-secrets
export CES_ORG_NAME='your organization name here'
```

Expected files inside `CES_SECRETS_DIR`:

- `APIKEY`
- `USER`
- `PASS`
- `URI`

`APIKEY`, `USER`, and `PASS` belong to your technical account and are provided by MFSR upon request. Do not forget to request whitelisting of your machine's egress IP.

`URI` is a machine-local JSON file that defines the concrete OD endpoint URLs used by the fetcher, for example:

```json
{
  "od001": "https://.../API_OD_001",
  "od002": "https://.../API_OD_002",
  "od003": "https://.../API_OD_003"
}
```

The individual API endpoints are also provided by MFSR along with the technical account credentials.

### 5. Run the pipeline locally

```bash
source /opt/datasets/runner-env.sh
./scripts/build-data.sh
```

### 6. Run in GitHub Actions

The workflow expects `/opt/datasets/runner-env.sh` to exist on the self-hosted runner machine. It loads that file, skips disabled modules, builds the output tree, and finally runs `make_index.py` on `DATA_ROOT`.

## What each module produces

### eGov

Under `EGOV_OUT_DIR`, the pipeline writes:

- public report JSON files (`KS.json`, `AS.json`, `ISVS.json`, `Projekt.json`, `InfraSluzba.json`, `KRIS.json`)
- CSV conversions of those reports
- cloud-service extraction outputs including:
  - `CloudSluzba.xlsx`
  - `CloudSluzba_curated.xlsx`
  - `CloudSluzba.json`
  - several raw and harmonized JSON files in `raw/`

### location

Under `LOCATION_OUT_DIR`, the pipeline writes:

- `kraje.csv`
- one CSV per region (`BSK.csv`, `TTSK.csv`, etc.)
- `_UNKNOWN.csv` and `_PENDING_UNKNOWN.csv` helper outputs when region assignment cannot be resolved immediately

### CES

Under `CES_EXPORT_OUT_DIR`, `ces-export` writes dataset outputs according to `ces-export/config/datasets.json`, including chunk files, merged files, manifests, and optional postprocessed derivatives.

## Orchestration details

`scripts/build-data.sh` currently passes:

- `egov/fetch-reports.sh --out-dir <EGOV_OUT_DIR>`
- `python egov/convert.py --data-dir <EGOV_OUT_DIR>`
- `python egov/cloud_services.py --out-dir <EGOV_OUT_DIR>`
- `python location/fetch-location.py --out-dir <LOCATION_OUT_DIR> --refresh-days <LOCATION_DATA_DAYS_REFRESH>`
- `ces-export/run.sh --out-dir <CES_EXPORT_OUT_DIR>`

The eGov cloud-services helper has additional optional tuning flags (`--created-at-from`, `--created-at-to`, `--window-target-count`, `--page-size`, `--probe-page-size`), but the top-level orchestrator deliberately uses its defaults.

## Sanity checks

Disable all modules:

```bash
DATA_ROOT=/tmp/datasets-test \
EGOV_OUT_DIR='' \
LOCATION_OUT_DIR='' \
CES_EXPORT_OUT_DIR='' \
PYTHON_BIN=python3 \
./scripts/build-data.sh
```

This should print three `skipped (output dir disabled)` messages and exit without trying to validate CES.
