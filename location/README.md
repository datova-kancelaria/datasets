# location export

Exports public Slovak territorial, street, address, and building data from the MV SR open-data catalog into a set of CSV files.

The source catalog is:

- `https://rageo.minv.sk/opendata/katalog.json`

No secrets are required.

## What this script does

The catalog contains several different dataset families. This script does **not** flatten them all into one schema. Instead, it separates them into:

- reference layers
- streets
- address points
- buildings

Dataset families used by the script:

- `nuts3.geojson` - kraje
- `lau1.geojson` - okresy
- `lau2.geojson` - obce
- `msa_by_lau1_*` - streets
- `maa_by_lau1_*` - address points
- `mba_by_lau2_*` - buildings

The script classifies datasets by filename and then writes each family into its own CSV schema.

## Script

- `fetch-location-v6.py`

## Usage

```bash
python fetch-location-v6.py --out-dir /path/to/location
```

Optional refresh control:

```bash
python fetch-location-v6.py --out-dir /path/to/location --refresh-days 30
```

If `--out-dir` is omitted, the default is:

```text
<repo>/data/location
```

If `--refresh-days` is omitted, the script uses:

1. `LOCATION_DATA_DAYS_REFRESH` from the environment, if set
2. otherwise `30`

## Output layout

The script writes the following structure:

```text
<out-dir>/
├── nuts3.csv
├── lau1.csv
├── lau2.csv
├── streets.csv
├── addresses/
│   ├── BBSK.csv
│   ├── BSK.csv
│   ├── KSK.csv
│   ├── NSK.csv
│   ├── PSK.csv
│   ├── TSK.csv
│   ├── TTSK.csv
│   └── ZSK.csv
└── buildings/
    ├── BBSK.csv
    ├── BSK.csv
    ├── KSK.csv
    ├── NSK.csv
    ├── PSK.csv
    ├── TSK.csv
    ├── TTSK.csv
    └── ZSK.csv
```

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

### `streets.csv`

Street reference table from `msa_by_lau1_*`.

Columns:

- `Ulica`
- `Ulica - ID`
- `Obec`
- `Obec - ID`
- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `Platné od`

Notes:

- `Ulica - ID` is taken from the street dataset `identifier`
- the script does not export `geometry_text` from streets
- `Časť obce` fields are omitted because they are not useful in the current street source

### `addresses/<kraj>.csv`

Address points from `maa_by_lau1_*`, split by kraj.

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

### `buildings/<kraj>.csv`

Building records from `mba_by_lau2_*`, split by kraj.

Columns:

- `ID budovy`
- `Typ budovy`
- `Typ budovy - kód`
- `Účel budovy`
- `Účel budovy - kód`
- `Súpisné číslo`
- `Obec`
- `Obec - ID`
- `Okres`
- `Okres - ID`
- `Kraj`
- `Kraj - ID`
- `Platné od`

Notes:

- `ID budovy` is taken from the building dataset `identifier`
- the current building source does not provide useful street linkage for export here
- `geometry_text` is not exported
- `identifier` and `objectid` were observed to be redundant in the building source, so only `ID budovy` is exported

## Processing flow

1. Fetch the catalog JSON.
2. Read dataset IRIs from `dcat:dataset`.
3. Classify dataset URLs by filename.
4. Fetch `nuts3`, `lau1`, and `lau2` first and build region lookup tables.
5. Fetch streets and write `streets.csv`.
6. Fetch buildings and write per-kraj building CSV files.
7. Fetch address points and write per-kraj address CSV files.

## Refresh logic

The script skips re-fetching when all expected outputs already exist and `nuts3.csv` is newer than the refresh threshold.

Expected outputs checked by the refresh logic:

- `nuts3.csv`
- `lau1.csv`
- `lau2.csv`
- `streets.csv`
- all `addresses/<kraj>.csv`
- all `buildings/<kraj>.csv`

If any expected file is missing, the script refreshes.

If all expected files exist and `nuts3.csv` is fresh enough, the script prints a skip message and exits successfully.

## Retry and failure behavior

Each HTTP/JSON fetch uses retry logic.

Typical dataset fetch settings:

- `tries=6`
- `timeout=30s`
- exponential backoff using `1.7 ** attempt`

Behavior:

- temporary request failures are retried several times
- invalid JSON is also retried
- after the final failed attempt, the script logs a `[FAIL]` message for that URL
- if the main catalog cannot be fetched and old outputs exist, the script keeps the existing outputs instead of deleting them

## Atomic output installation

The script writes everything into a staging directory first:

```text
.<out-dir-name>.tmp
```

Only after processing finishes successfully does it replace the real output directory contents.

This avoids half-written output sets.

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

This prevents mixing the old layout with the new nested layout.

## Current assumptions and limitations

- dataset type is determined from the dataset filename, not inferred from individual feature contents
- building exports currently omit street columns because the building source does not provide reliable street linkage for useful export
- `geometry_text` found in some sources is ignored because it is not useful for the current CSV exports
- rows are grouped into per-kraj files using the region information present in the source data or derived from the reference layers
