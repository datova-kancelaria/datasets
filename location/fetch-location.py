from __future__ import annotations

import argparse
import csv
import shutil
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


def has_existing_outputs(out_dir: Path) -> bool:
    return any(path.exists() for path in expected_files(out_dir))


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
                time.sleep(backoff**attempt)
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


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def feature_to_row(feature: dict[str, Any]) -> list[str]:
    properties = feature.get("properties") or {}
    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or []
    x = coords[0] if len(coords) >= 2 else ""
    y = coords[1] if len(coords) >= 2 else ""

    return [
        clean_str(properties.get("identifier")),
        clean_str(properties.get("nuts3_name")),
        clean_str(properties.get("nuts3_id")),
        clean_str(properties.get("lau1_name")),
        clean_str(properties.get("lau1_id")),
        clean_str(properties.get("lau2_name")),
        clean_str(properties.get("lau2_id")),
        clean_str(properties.get("district_name")),
        clean_str(properties.get("streetname")),
        clean_str(properties.get("propertyregistrationnumber")),
        clean_str(properties.get("orientationnumber")),
        clean_str(properties.get("postalcode")),
        str(x),
        str(y),
    ]


def open_region_writers(out_dir: Path):
    region_files: dict[str, Any] = {}
    region_writers: dict[str, csv.writer] = {}

    for kraj, abb in name_to_abb.items():
        path = out_dir / f"{abb}.csv"
        f = path.open("w", newline="", encoding="utf-8")
        w = csv.writer(f)
        w.writerow(HEADER)
        region_files[kraj] = f
        region_writers[kraj] = w

    unknown_path = out_dir / "_UNKNOWN.csv"
    unknown_file = unknown_path.open("w", newline="", encoding="utf-8")
    unknown_writer = csv.writer(unknown_file)
    unknown_writer.writerow(HEADER)

    pending_path = out_dir / "_PENDING_UNKNOWN.csv"
    pending_file = pending_path.open("w", newline="", encoding="utf-8")
    pending_writer = csv.writer(pending_file)
    pending_writer.writerow(HEADER)

    return (
        region_files,
        region_writers,
        unknown_file,
        unknown_writer,
        unknown_path,
        pending_file,
        pending_writer,
        pending_path,
    )


def file_has_data_rows(path: Path) -> bool:
    if not path.exists():
        return False
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        return next(reader, None) is not None


def append_csv_without_header(dst_writer: csv.writer, src_path: Path) -> None:
    if not src_path.exists():
        return

    with src_path.open("r", newline="", encoding="utf-8") as fin:
        reader = csv.reader(fin)
        next(reader, None)
        for row in reader:
            dst_writer.writerow(row)


def learn_region_maps(
    row: list[str],
    okres_id_to_region: dict[str, tuple[str, str]],
    okres_name_to_region: dict[str, tuple[str, str]],
    obec_id_to_region: dict[str, tuple[str, str]],
    obec_name_to_region: dict[str, tuple[str, str]],
) -> None:
    kraj = row[1].strip()
    kraj_id = row[2].strip()
    okres = row[3].strip()
    okres_id = row[4].strip()
    obec = row[5].strip()
    obec_id = row[6].strip()

    if not kraj or not kraj_id:
        return

    region = (kraj, kraj_id)

    if okres_id:
        okres_id_to_region.setdefault(okres_id, region)
    if okres:
        okres_name_to_region.setdefault(okres, region)
    if obec_id:
        obec_id_to_region.setdefault(obec_id, region)
    if obec:
        obec_name_to_region.setdefault(obec, region)


def try_fill_missing_kraj(
    row: list[str],
    okres_id_to_region: dict[str, tuple[str, str]],
    okres_name_to_region: dict[str, tuple[str, str]],
    obec_id_to_region: dict[str, tuple[str, str]],
    obec_name_to_region: dict[str, tuple[str, str]],
) -> bool:
    kraj = row[1].strip()

    if kraj in name_to_abb:
        return True

    okres = row[3].strip()
    okres_id = row[4].strip()
    obec = row[5].strip()
    obec_id = row[6].strip()

    region: tuple[str, str] | None = None

    if okres_id and okres_id in okres_id_to_region:
        region = okres_id_to_region[okres_id]
    elif obec_id and obec_id in obec_id_to_region:
        region = obec_id_to_region[obec_id]
    elif okres and okres in okres_name_to_region:
        region = okres_name_to_region[okres]
    elif obec and obec in obec_name_to_region:
        region = obec_name_to_region[obec]

    if region is None:
        return False

    row[1], row[2] = region
    return True


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


def resolve_pending_rows(
    pending_path: Path,
    unknown_writer: csv.writer,
    region_writers: dict[str, csv.writer],
    seen_unknown_kraje: set[str],
    okres_id_to_region: dict[str, tuple[str, str]],
    okres_name_to_region: dict[str, tuple[str, str]],
    obec_id_to_region: dict[str, tuple[str, str]],
    obec_name_to_region: dict[str, tuple[str, str]],
) -> None:
    if not pending_path.exists():
        return

    with pending_path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            filled = try_fill_missing_kraj(
                row,
                okres_id_to_region,
                okres_name_to_region,
                obec_id_to_region,
                obec_name_to_region,
            )

            if filled:
                learn_region_maps(
                    row,
                    okres_id_to_region,
                    okres_name_to_region,
                    obec_id_to_region,
                    obec_name_to_region,
                )

                kraj = row[1].strip()
                if kraj in region_writers:
                    region_writers[kraj].writerow(row)
                    continue

            unknown_writer.writerow(row)
            kraj = row[1].strip()
            if kraj:
                seen_unknown_kraje.add(kraj)


def remove_if_header_only(path: Path) -> None:
    if not path.exists():
        return
    if not file_has_data_rows(path):
        path.unlink()


def count_data_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader, None)
        return sum(1 for _ in reader)


def count_generated_rows(out_dir: Path, unknown_path: Path) -> int:
    total = 0
    for abb in name_to_abb.values():
        total += count_data_rows(out_dir / f"{abb}.csv")
    total += count_data_rows(unknown_path)
    return total


def staging_dir_for(out_dir: Path) -> Path:
    return out_dir.parent / f".{out_dir.name}.tmp"


def prepare_staging_dir(out_dir: Path) -> Path:
    staging_dir = staging_dir_for(out_dir)
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir


def install_outputs(staging_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    for path in expected_files(out_dir):
        path.unlink(missing_ok=True)

    unknown_target = out_dir / "_UNKNOWN.csv"
    pending_target = out_dir / "_PENDING_UNKNOWN.csv"
    unknown_target.unlink(missing_ok=True)
    pending_target.unlink(missing_ok=True)

    for src in staging_dir.glob("*.csv"):
        shutil.move(str(src), str(out_dir / src.name))


def cleanup_staging_dir(staging_dir: Path) -> None:
    if staging_dir.exists():
        shutil.rmtree(staging_dir)


def keep_existing_outputs_message(reason: str, out_dir: Path) -> int:
    print(f"NUTS refresh skipped: {reason}")
    print(f"Keeping existing outputs in {out_dir}")
    return 0


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
    if not uris:
        if has_existing_outputs(out_dir):
            return keep_existing_outputs_message(
                f"failed to obtain dataset catalog from {URI_root}", out_dir
            )
        print(f"NUTS refresh failed: could not obtain dataset catalog from {URI_root}")
        return 1

    staging_dir = prepare_staging_dir(out_dir)

    try:
        print("Opening region files...")
        (
            region_files,
            region_writers,
            unknown_file,
            unknown_writer,
            unknown_path,
            pending_file,
            pending_writer,
            pending_path,
        ) = open_region_writers(staging_dir)

        seen_unknown_kraje: set[str] = set()

        okres_id_to_region: dict[str, tuple[str, str]] = {}
        okres_name_to_region: dict[str, tuple[str, str]] = {}
        obec_id_to_region: dict[str, tuple[str, str]] = {}
        obec_name_to_region: dict[str, tuple[str, str]] = {}

        rows_written = 0

        try:
            s = requests.Session()

            for uri in tqdm(uris, desc="Fetching datasets", unit="dataset"):
                r = fetch_json_with_retry(
                    uri,
                    session=s,
                    tries=6,
                    timeout=30.0,
                    backoff=1.7,
                )
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

                    learn_region_maps(
                        row,
                        okres_id_to_region,
                        okres_name_to_region,
                        obec_id_to_region,
                        obec_name_to_region,
                    )

                    filled = try_fill_missing_kraj(
                        row,
                        okres_id_to_region,
                        okres_name_to_region,
                        obec_id_to_region,
                        obec_name_to_region,
                    )

                    if filled:
                        learn_region_maps(
                            row,
                            okres_id_to_region,
                            okres_name_to_region,
                            obec_id_to_region,
                            obec_name_to_region,
                        )

                    kraj = row[1].strip()

                    if kraj in region_writers:
                        region_writers[kraj].writerow(row)
                        rows_written += 1
                    else:
                        pending_writer.writerow(row)

                del r
                del current

            resolve_pending_rows(
                pending_path,
                unknown_writer,
                region_writers,
                seen_unknown_kraje,
                okres_id_to_region,
                okres_name_to_region,
                obec_id_to_region,
                obec_name_to_region,
            )

        finally:
            for f in region_files.values():
                f.close()
            unknown_file.close()
            pending_file.close()

        remove_if_header_only(unknown_path)
        remove_if_header_only(pending_path)

        generated_rows = count_generated_rows(staging_dir, unknown_path)
        if generated_rows == 0 and rows_written == 0:
            if has_existing_outputs(out_dir):
                return keep_existing_outputs_message(
                    "catalog was fetched but no dataset rows were produced", out_dir
                )
            print("NUTS refresh failed: catalog was fetched but no dataset rows were produced")
            return 1

        if seen_unknown_kraje:
            print("Warning: Kraj values not in name_to_abb:")
            for kraj in sorted(seen_unknown_kraje):
                print(f"  - {kraj}")

        print("Building kraje.csv...")
        build_kraje_csv(staging_dir, unknown_path)
        install_outputs(staging_dir, out_dir)
        return 0

    finally:
        cleanup_staging_dir(staging_dir)


if __name__ == "__main__":
    raise SystemExit(main())
