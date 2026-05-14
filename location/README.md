# location export

Exports public Slovak territorial, street, address, building, and district data from the MV SR open-data catalog into CSV files and, by default, matching XLSX files.

The source catalog is:

- `https://rageo.minv.sk/opendata/katalog.json`

No secrets are required.

## Script

```bash
location/fetch-location.py
```

Run from the repository root, for example:

```bash
python3 location/fetch-location.py --out-dir location-out
```

If `--out-dir` is omitted, the default is:

```text
<repo>/data/location
```

## What the script does

The catalog currently points mostly to dataset metadata records, not directly to GeoJSON files. The script therefore does this:

1. Fetches the catalog.
2. Reads dataset IRIs from `dcat:dataset`.
3. Resolves each metadata record to a GeoJSON distribution URL, preferring `dcat:downloadURL` and falling back to `dcat:accessURL`.
4. Classifies the resolved GeoJSON URLs by filename.
5. Fetches the per-LAU2 address, street, and building files.
6. Derives reference tables for kraje, okresy, and obce from the source properties.
7. Builds district and street-to-district lookup tables.
8. Writes sorted CSV outputs.
9. Writes styled XLSX copies of all generated CSV files unless disabled.

The script still supports old-style direct GeoJSON catalog entries as a fallback.

## Dataset families

Preferred current dataset families:

- `address_by_lau2_*` - address points
- `street_by_lau2_*` - streets
- `building_by_lau2_*` - buildings

Older naming fallback:

- `maa_by_lau2_*` - address points
- `msa_by_lau2_*` - streets
- `mba_by_lau2_*` - buildings

Reference layers are also supported when present:

- `nuts3.geojson` - kraje
- `lau1.geojson` - okresy
- `lau2.geojson` - obce

Aggregate duplicate datasets such as `*_by_lau1_*` and `*_by_nuts3_*` are skipped when per-LAU2 datasets are available, to avoid duplicate rows.

## Usage

Basic run:

```bash
python3 location/fetch-location.py --out-dir location-out
```

Force refresh regardless of age:

```bash
python3 location/fetch-location.py --out-dir location-out --refresh-days -1
```

Skip XLSX generation:

```bash
python3 location/fetch-location.py --out-dir location-out --skip-xlsx
```

Equivalent alias:

```bash
python3 location/fetch-location.py --out-dir location-out --skip-excel
```

Dump downloaded catalog, metadata, and GeoJSON payloads for inspection:

```bash
python3 location/fetch-location.py --out-dir location-out --debug
```

Optional refresh control:

```bash
python3 location/fetch-location.py --out-dir location-out --refresh-days 30
```

If `--refresh-days` is omitted, the script uses:

1. `LOCATION_DATA_DAYS_REFRESH` from the environment, if set
2. otherwise `30`

## Output layout

With default XLSX generation enabled, the output structure is:

```text
<out-dir>/
├── nuts3.csv
├── nuts3.xlsx
├── lau1.csv
├── lau1.xlsx
├── lau2.csv
├── lau2.xlsx
├── districts.csv
├── districts.xlsx
├── streets.csv
├── streets.xlsx
├── addresses/
│   ├── BBSK.csv
│   ├── BBSK.xlsx
│   ├── BSK.csv
│   ├── BSK.xlsx
│   ├── KSK.csv
│   ├── KSK.xlsx
│   ├── NSK.csv
│   ├── NSK.xlsx
│   ├── PSK.csv
│   ├── PSK.xlsx
│   ├── TSK.csv
│   ├── TSK.xlsx
│   ├── TTSK.csv
│   ├── TTSK.xlsx
│   ├── ZSK.csv
│   └── ZSK.xlsx
├── buildings/
│   ├── BBSK.csv
│   ├── BBSK.xlsx
│   ├── BSK.csv
│   ├── BSK.xlsx
│   ├── KSK.csv
│   ├── KSK.xlsx
│   ├── NSK.csv
│   ├── NSK.xlsx
│   ├── PSK.csv
│   ├── PSK.xlsx
│   ├── TSK.csv
│   ├── TSK.xlsx
│   ├── TTSK.csv
│   ├── TTSK.xlsx
│   ├── ZSK.csv
│   └── ZSK.xlsx
└── audit/
    ├── street_district_conflicts.csv
    ├── street_district_conflicts.xlsx
    ├── street_context_conflicts.csv
    ├── street_context_conflicts.xlsx
    ├── district_conflicts.csv
    └── district_conflicts.xlsx
```

Only audit files that contain rows are written. If `--debug` is used, the final output may also include a `debug/` directory.

If `--skip-xlsx` / `--skip-excel` is used, no XLSX files are written.

Region abbreviations:

- `BSK` - Bratislavský
- `TTSK` - Trnavský
- `TSK` - Trenčiansky
- `NSK` - Nitriansky
- `ZSK` - Žilinský
- `BBSK` - Banskobystrický
- `PSK` - Prešovský
- `KSK` - Košický

## CSV schemas

### `nuts3.csv`

Reference table for kraje.

Columns:

- `Kraj`
- `Kraj - ID`
- `ID objektu`
- `IČO`
- `Platné od`

### `lau1.csv`

Reference table for okresy.

Columns:

- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `ID objektu`
- `IČO`
- `Platné od`

### `lau2.csv`

Reference table for obce.

Columns:

- `Obec`
- `Obec - ID`
- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `ID objektu`
- `IČO`
- `Platné od`

### `districts.csv`

Reference table for časti obce.

Columns:

- `Časť obce`
- `Časť obce - ID`
- `Obec`
- `Obec - ID`
- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `Platné od`

Notes:

- districts are derived from address and building datasets
- if the same district ID appears with conflicting context, the conflict is written to `audit/district_conflicts.csv`

### `streets.csv`

Street reference table.

Columns:

- `Ulica`
- `Ulica - ID`
- `Časť obce`
- `Časť obce - ID`
- `Obec`
- `Obec - ID`
- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `Platné od`

Notes:

- `Ulica - ID` is taken from the street dataset `identifier`
- district fields are taken from the street dataset when available and enriched from address-derived street links
- when one street belongs to multiple districts, district names and IDs are written as comma-separated values
- CSV quoting is handled by `csv.DictWriter`
- `geometry_text` from the street source is not exported

### `addresses/<kraj>.csv`

Address points split by kraj.

Columns:

- `ID budovy`
- `ID objektu`
- `Ulica`
- `Ulica - ID`
- `Súpisné číslo`
- `Orientačné číslo`
- `PSČ`
- `Časť obce`
- `Časť obce - ID`
- `Obec`
- `Obec - ID`
- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `Platné od`
- `ADRBOD_X`
- `ADRBOD_Y`
- `URI`

Notes:

- `ID budovy` is taken from the address dataset `identifier`
- `ID objektu` is taken from `objectid`
- coordinates come from the GeoJSON geometry coordinates
- address rows are also used to infer street-to-district links and district records

### `buildings/<kraj>.csv`

Building records split by kraj.

Columns:

- `ID budovy`
- `Typ budovy`
- `Typ budovy - kód`
- `Účel budovy`
- `Účel budovy - kód`
- `Súpisné číslo`
- `Časť obce`
- `Časť obce - ID`
- `Obec`
- `Obec - ID`
- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `Platné od`

Notes:

- `ID budovy` is taken from the building dataset `identifier`
- building rows are also used to derive district records
- `geometry_text` is not exported
- `identifier` and `objectid` were observed to be redundant in the building source, so only `ID budovy` is exported

## Audit outputs

Audit files are written under `audit/` only when there are rows to report.

### `audit/street_district_conflicts.csv`

Written when one street ID is observed with conflicting district IDs or names.

Columns:

- `Ulica - ID`
- `Ulica`
- `Obec - ID`
- `Obec`
- `Okres - ID`
- `Okres`
- `Kraj - ID`
- `Kraj`
- `Prvý district_id`
- `Prvý district_name`
- `Konfliktné district_id hodnoty`
- `Konfliktné district_name hodnoty`

### `audit/street_context_conflicts.csv`

Written when one street ID is observed with conflicting context, such as municipality, district, or region metadata.

Columns:

- `Ulica - ID`
- `Ulica`
- `Obec - ID`
- `Obec`
- `Okres - ID`
- `Okres`
- `Kraj - ID`
- `Kraj`
- `Konfliktné polia`

### `audit/district_conflicts.csv`

Written when one district ID is observed with conflicting context.

Columns:

- `Časť obce - ID`
- `Časť obce`
- `Obec - ID`
- `Obec`
- `Okres - ID`
- `Okres`
- `Kraj - ID`
- `Kraj`
- `Konfliktné polia`

## XLSX output

For every generated CSV file, the script creates a matching XLSX file by default.

XLSX formatting:

- dark blue header row
- white header text
- frozen header pane (`A2`)
- autofilter over the whole table
- estimated column widths
- alternating data-row colors implemented with Excel conditional formatting

The alternating row colors are:

- first data row: `#ffffcc`
- second data row: `#e6f7ff`

The row striping is stored as conditional formatting rules instead of per-cell fills. This keeps generation much faster and avoids writing millions of individually styled cells.

Use `--skip-xlsx` or `--skip-excel` to disable XLSX generation entirely.

## Sorting

Generated CSV files are sorted before XLSX conversion.

Sort rules:

- `nuts3.csv`: `Kraj`
- `lau1.csv`: `Okres`, then `Kraj`
- `lau2.csv`: `Obec`, then `Kraj`, then `Okres`
- `districts.csv`: `Časť obce`, then `Obec`, `Okres`, `Kraj`; empty district names last
- `streets.csv`: `Ulica`, then `Obec`, then `Časť obce`, then `Ulica - ID`
- `addresses/<kraj>.csv`: `Ulica`, then natural sort on `Orientačné číslo`, then `ID budovy`
- `buildings/<kraj>.csv`: `Obec`, then `Časť obce` with empty values last, then natural sort on `Súpisné číslo`, then `ID budovy`

Sorting is done on CSV data before XLSX files are generated.

## Processing flow

1. Fetch the catalog JSON.
2. Read dataset metadata IRIs from `dcat:dataset`.
3. Resolve each metadata record to a GeoJSON distribution URL.
4. Classify resolved GeoJSON URLs by filename.
5. Skip aggregate duplicate datasets when per-LAU2 datasets exist.
6. Fetch reference layers if present.
7. Fetch address datasets first, derive reference maps, districts, and street links.
8. Fetch street datasets and enrich district columns from address-derived street links.
9. Fetch building datasets and derive additional district records.
10. Write district and audit outputs.
11. Close all CSV files.
12. If no address or building rows were produced, keep existing outputs when possible.
13. Sort generated CSV files.
14. Generate XLSX copies unless disabled.
15. Atomically install the staged output directory.

## Refresh logic

The script skips re-fetching when all expected outputs already exist and `nuts3.csv` is newer than the refresh threshold.

When XLSX generation is enabled, the expected outputs include both CSV and XLSX files. When `--skip-xlsx` is used, only CSV files are required by the refresh check.

If any expected file is missing, the script refreshes.

If all expected files exist and `nuts3.csv` is fresh enough, the script prints a skip message and exits successfully.

## Retry and failure behavior

Each HTTP/JSON fetch uses retry logic.

Timeouts:

- catalog: connect timeout `15s`, read timeout `120s`
- metadata records: connect timeout `15s`, read timeout `60s`
- GeoJSON datasets: connect timeout `15s`, read timeout `600s`

Typical dataset fetch settings:

- `tries=6`
- exponential backoff using `1.7 ** attempt`

Behavior:

- temporary request failures are retried several times
- invalid JSON is also retried
- after the final failed attempt, the script logs a `[FAIL]` message for that URL
- if the main catalog cannot be fetched and old outputs exist, the script keeps the existing outputs instead of deleting them
- if the catalog is fetched but no GeoJSON URLs can be resolved, old outputs are kept when available
- if no address or building rows are produced, old outputs are kept when available
- individual failed GeoJSON files are skipped; the run is considered successful if substantial address/building output is produced

## Atomic output installation

The script writes everything into a staging directory first:

```text
.<out-dir-name>.tmp
```

Only after processing, sorting, optional XLSX generation, and safety checks does it replace the real output directory contents.

This avoids half-written output sets.

## Debug output

With `--debug`, downloaded JSON/GeoJSON payloads are written under the staging output directory and then installed with the final outputs:

```text
<out-dir>/debug/catalog/
<out-dir>/debug/metadata/
<out-dir>/debug/geojson/
```

Debug filenames are derived from URL path basenames, with collision handling.

## Encoding handling

The script writes CSV files as:

- `utf-8-sig`

This makes the files easier to open correctly in Excel.

The script also:

- forces HTTP response decoding to UTF-8 before parsing JSON
- applies a small mojibake repair heuristic for common broken Slovak diacritics such as `NebytovĂˇ budova`

## Legacy cleanup

Before installing new outputs, the script removes older flat-layout files such as:

- `kraje.csv`
- `okresy.csv`
- `obce.csv`
- `ulice.csv`
- old flat per-kraj CSV files like `BSK.csv` and `BSK_buildings.csv`

It also removes previous `addresses/`, `buildings/`, `audit/`, and `debug/` directories before installing the new output set.

This prevents mixing old and new layouts.

## Current assumptions and limitations

- dataset type is determined from the resolved GeoJSON filename, not inferred from individual feature contents
- per-LAU2 address, street, and building datasets are preferred; aggregate datasets are skipped to avoid duplicates
- reference tables may be derived from address source properties when standalone `nuts3`, `lau1`, or `lau2` layers are absent
- street district membership can be many-to-one or many-to-many; multiple districts are exported as comma-separated values
- `geometry_text` found in some sources is ignored because it is not useful for the current CSV/XLSX exports
- rows are grouped into per-kraj files using the region information present in the source data or derived from the reference layers
