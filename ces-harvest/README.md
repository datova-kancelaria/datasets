# CES harvest pipeline (refactored)

CES data harvesting pipeline.

## Highlights

- Dataset scheduling is driven by JSON config
- Range paging/windowing is per-format and configurable (`none`, `days`, `calendar_month`, `calendar_quarter`, `calendar_year`).
- XML can use a conservative `keep_chunks` strategy for years/ranges that do not merge cleanly.
- Credentials are loaded at runtime
- The runner returns structured results instead of pretending every job yields one merged file.

## Package layout

- `harvest/__main__.py` — CLI entrypoint
- `harvest/settings.py` — URLs, credentials, session setup
- `harvest/models.py` — dataclasses / types
- `harvest/ces_api.py` — OD_001 / OD_002 / OD_003 transport
- `harvest/orgs.py` — org matching and org-code cache
- `harvest/io_utils.py` — atomic write + metadata helpers
- `harvest/date_rules.py` — date/window helpers
- `harvest/dataset_config.py` — config loading/normalization
- `harvest/planner.py` — config -> jobs
- `harvest/mergers.py` — CSV merge, XML merge, chunk manifest
- `harvest/postprocess.py` — CSV->XLSX, RDF/XML->JSON-LD
- `harvest/runner.py` — job execution

## Example usage

```bash
python -m harvest \
  --config config/datasets.example.json \
  --org-name MIRRI
```

List orgs:

```bash
python -m harvest \
  --config config/datasets.example.json \
  --list-orgs
```

Dry run:

```bash
python -m harvest \
  --config config/datasets.example.json \
  --org-name MIRRI \
  --dry-run
```

## Credentials

- `CREDENTIALS_DIRECTORY` must be set
- files expected inside it:
  - `APIKEY`
  - `USER`
  - `PASS`

Optional env vars:

- `CES_TRUST_ENV=1` — allow `requests` to use proxy env vars
- `CES_ORG_NAME`
- `CES_HIERARCHY_NODE_CODE`

## Config shape

See `config/datasets.json`.

Important fields:

- `datasets.<name>.schedules[]`
- `datasets.<name>.formats.csv`
- `datasets.<name>.formats.xml`

Format window examples:

```json
{ "mode": "none", "size": 1 }
{ "mode": "days", "size": 30 }
{ "mode": "calendar_month", "size": 1 }
{ "mode": "calendar_quarter", "size": 1 }
```

Merge strategies:

- `csv_header`
- `rdfxml_graph`
- `concat`
- `keep_chunks`
- `skip_if_chunked`

## XML note

For problematic XML years such as 2025, configure XML like this:

```json
{
  "enabled": true,
  "window": { "mode": "calendar_month", "size": 1 },
  "merge_strategy": "keep_chunks",
  "postprocess": [],
  "keep_chunks": true
}
```

That preserves the XML chunks and writes a manifest without lying that a merged XML exists.
