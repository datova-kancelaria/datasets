from __future__ import annotations

import argparse
import csv
import time
from pathlib import Path
from typing import Any

import requests
from tqdm import tqdm

URI_root = "https://rageo.minv.sk/opendata/katalog.json"

name_to_abb = {
    "Banskobystrický": "BBSK",
    "Bratislavský": "BSK",
    "Nitriansky": "NSK",
    "Košický": "KSK",
    "Prešovský": "PSK",
    "Trenčiansky": "TSK",
    "Trnavský": "TTSK",
    "Žilinský": "ZSK",
}

region_order = [
    "Bratislavský",
    "Trnavský",
    "Trenčiansky",
    "Nitriansky",
    "Žilinský",
    "Banskobystrický",
    "Prešovský",
    "Košický",
]

HEADER = [
    "Identifikátor",
    "Kraj",
    "ID kraja",
    "Okres",
    "ID Okresu",
    "Obec",
    "ID obce",
    "Časť obce",
    "Ulica",
    "Súpisné číslo",
    "Orientačné číslo celé",
    "PSČ",
    "ADRBOD_X",
    "ADRBOD_Y",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "location",
        help="Directory where NUTS/location CSV files will be written",
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=None,
        help="Skip refresh when kraje.csv is newer than this many days",
    )
    return parser.parse_args()


def effective_refresh_days(cli_value: int | None) -> int:
    if cli_value is not None:
        return cli_value
    import os
    return int(os.environ.get("LOCATION_DATA_DAYS_REFRESH", "30"))


def is_fresh(path: Path, refresh_days: int) -> bool:
    if refresh_days < 0 or not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < refresh_days * 86400


def expected_files(out_dir: Path) -> list[Path]:
    return [out_dir / "kraje.csv"] + [
        out_dir / f"{abb}.csv" for abb in name_to_abb.values()
    ]


def should_refresh(out_dir: Path, refresh_days: int) -> bool:
    files = expected_files(out_dir)

    if any(not p.exists() for p in files):
        return True

    sentinel = out_dir / "kraje.csv"
    return not is_fresh(sentinel, refresh_days)


def fetch_json_with_retry(
    url: str,
    *,
    session: requests.Session | None = None,
    tries: int = 5,
    timeout: float = 30.0,
    backoff: float = 1.5,
) -> dict[str, Any] | None:
    s = session or requests

    for attempt in range(tries):
        try:
            resp = s.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt < tries - 1:
                time.sleep(backoff ** attempt)
            else:
                print(f"[FAIL] {url} after {tries} tries: {e}")

    return None


def parse_dataset(uri: str) -> list[str]:
    r = fetch_json_with_retry(uri)
    if r is None:
        return []

    graph = r.get("@graph", [])
    for g in graph:
        ds = g.get("dcat:dataset")
        if ds is not None:
            return [o["iri"] for o in ds if isinstance(o, dict) and "iri" in o]
    return []


def feature_to_row(feature: dict[str, Any]) -> list[str]:
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or []
    x = coords[0] if len(coords) >= 2 else ""
    y = coords[1] if len(coords) >= 2 else ""

    return [
        str(properties.get("identifier", "")),
        str(properties.get("nuts3_name", "")),
        str(properties.get("nuts3_id", "")),
        str(properties.get("lau1_name", "")),
        str(properties.get("lau1_id", "")),
        str(properties.get("lau2_name", "")),
        str(properties.get("lau2_id", "")),
        str(properties.get("district_name", "")),
        str(properties.get("streetname", "")),
        str(properties.get("propertyregistrationnumber", "")),
        str(properties.get("orientationnumber", "")),
        str(properties.get("postalcode", "")),
        str(x),
        str(y),
    ]


def open_region_writers(out_dir: Path):
    files: dict[str, Any] = {}
    writers: dict[str, csv.writer] = {}

    for kraj, abb in name_to_abb.items():
        path = out_dir / f"{abb}.csv"
        f = path.open("w", newline="", encoding="utf-8")
        w = csv.writer(f)
        w.writerow(HEADER)
        files[kraj] = f
        writers[kraj] = w

    unknown_path = out_dir / "_UNKNOWN.csv"
    unknown_file = unknown_path.open("w", newline="", encoding="utf-8")
    unknown_writer = csv.writer(unknown_file)
    unknown_writer.writerow(HEADER)

    return files, writers, unknown_file, unknown_writer, unknown_path


def file_has_data_rows(path: Path) -> bool:
    if not path.exists():
        return False
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)  # header
        return next(reader, None) is not None


def append_csv_without_header(dst_writer: csv.writer, src_path: Path) -> None:
    if not src_path.exists():
        return

    with src_path.open("r", newline="", encoding="utf-8") as fin:
        reader = csv.reader(fin)
        next(reader, None)  # skip header
        for row in reader:
            dst_writer.writerow(row)


def build_kraje_csv(out_dir: Path, unknown_path: Path) -> None:
    target = out_dir / "kraje.csv"
    with target.open("w", newline="", encoding="utf-8") as fout:
        writer = csv.writer(fout)
        writer.writerow(HEADER)

        for kraj in region_order:
            abb = name_to_abb[kraj]
            append_csv_without_header(writer, out_dir / f"{abb}.csv")

        if file_has_data_rows(unknown_path):
            append_csv_without_header(writer, unknown_path)


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir
    refresh_days = effective_refresh_days(args.refresh_days)

    out_dir.mkdir(parents=True, exist_ok=True)

    if not should_refresh(out_dir, refresh_days):
        print(
            f"Skipping NUTS refresh: all expected files exist and are newer than {refresh_days} days"
        )
        return 0

    print("Obtaining URIs...")
    uris = parse_dataset(URI_root)

    print("Opening region files...")
    region_files, region_writers, unknown_file, unknown_writer, unknown_path = open_region_writers(out_dir)
    seen_unknown_kraje: set[str] = set()

    try:
        s = requests.Session()

        for uri in tqdm(uris, desc="Fetching datasets", unit="dataset"):
            r = fetch_json_with_retry(uri, session=s, tries=6, timeout=30.0, backoff=1.7)
            if r is None:
                continue

            current = r.get("features", [])
            if not current:
                print(f"Warning: features in {uri} is empty/does not exist!")
                continue

            for feature in current:
                if not isinstance(feature, dict):
                    continue

                row = feature_to_row(feature)
                kraj = row[1].strip()

                if kraj in region_writers:
                    region_writers[kraj].writerow(row)
                else:
                    unknown_writer.writerow(row)
                    if kraj:
                        seen_unknown_kraje.add(kraj)

            del r
            del current

    finally:
        for f in region_files.values():
            f.close()
        unknown_file.close()

    if seen_unknown_kraje:
        print("Warning: Kraj values not in name_to_abb:")
        for kraj in sorted(seen_unknown_kraje):
            print(f"  - {kraj}")

    print("Building kraje.csv...")
    build_kraje_csv(out_dir, unknown_path)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
