from __future__ import annotations

import argparse
import csv
import os
import shutil
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import requests
from tqdm import tqdm

URI_ROOT = "https://rageo.minv.sk/opendata/katalog.json"

NAME_TO_ABB = {
    "Banskobystrický": "BBSK",
    "Bratislavský": "BSK",
    "Nitriansky": "NSK",
    "Košický": "KSK",
    "Prešovský": "PSK",
    "Trenčiansky": "TSK",
    "Trnavský": "TTSK",
    "Žilinský": "ZSK",
}

KRAJE_HEADER = [
    "Kraj",
    "Kraj - ID",
    "ID objektu",
    "IČO",
    "Platné od",
]

OKRESY_HEADER = [
    "Okres",
    "Okres - ID",
    "Kraj",
    "Kraj - ID",
    "ID objektu",
    "IČO",
    "Platné od",
]

OBCE_HEADER = [
    "Obec",
    "Obec - ID",
    "Okres",
    "Okres - ID",
    "Kraj",
    "Kraj - ID",
    "ID objektu",
    "IČO",
    "Platné od",
]

ULICE_HEADER = [
    "Ulica",
    "Ulica - ID",
    "Obec",
    "Obec - ID",
    "Okres",
    "Okres - ID",
    "Kraj",
    "Kraj - ID",
    "Platné od",
]

ADDRESS_HEADER = [
    "ID budovy",
    "ID objektu",
    "Ulica",
    "Ulica - ID",
    "Súpisné číslo",
    "Orientačné číslo",
    "PSČ",
    "Časť obce",
    "Časť obce - ID",
    "Obec",
    "Obec - ID",
    "Okres",
    "Okres - ID",
    "Kraj",
    "Kraj - ID",
    "Platné od",
    "ADRBOD_X",
    "ADRBOD_Y",
    "URI",
]

BUILDING_HEADER = [
    "ID budovy",
    "Typ budovy",
    "Typ budovy - kód",
    "Účel budovy",
    "Účel budovy - kód",
    "Súpisné číslo",
    "Obec",
    "Obec - ID",
    "Okres",
    "Okres - ID",
    "Kraj",
    "Kraj - ID",
    "Platné od",
]


@dataclass(frozen=True)
class RegionInfo:
    kraj_name: str
    kraj_id: str
    okres_name: str = ""
    okres_id: str = ""
    obec_name: str = ""
    obec_id: str = ""


@dataclass(frozen=True)
class DatasetGroups:
    nuts3: list[str]
    lau1: list[str]
    lau2: list[str]
    msa: list[str]
    maa: list[str]
    mba: list[str]
    other: list[str]


class CsvSink:
    def __init__(self, path: Path, header: list[str]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._file = path.open("w", newline="", encoding="utf-8-sig")
        self._writer = csv.DictWriter(self._file, fieldnames=header)
        self._writer.writeheader()
        self.rows_written = 0

    def write(self, row: dict[str, Any]) -> None:
        clean = {key: clean_str(row.get(key)) for key in self._writer.fieldnames}
        self._writer.writerow(clean)
        self.rows_written += 1

    def close(self) -> None:
        self._file.close()


class OutputManager:
    def __init__(self, out_dir: Path) -> None:
        self.out_dir = out_dir
        self.kraje = CsvSink(out_dir / "nuts3.csv", KRAJE_HEADER)
        self.okresy = CsvSink(out_dir / "lau1.csv", OKRESY_HEADER)
        self.obce = CsvSink(out_dir / "lau2.csv", OBCE_HEADER)
        self.ulice = CsvSink(out_dir / "streets.csv", ULICE_HEADER)
        self.addresses: dict[str, CsvSink] = {}
        self.buildings: dict[str, CsvSink] = {}

        addresses_dir = out_dir / "addresses"
        buildings_dir = out_dir / "buildings"
        for kraj_name, abb in NAME_TO_ABB.items():
            self.addresses[kraj_name] = CsvSink(addresses_dir / f"{abb}.csv", ADDRESS_HEADER)
            self.buildings[kraj_name] = CsvSink(buildings_dir / f"{abb}.csv", BUILDING_HEADER)

    def close(self) -> None:
        self.kraje.close()
        self.okresy.close()
        self.obce.close()
        self.ulice.close()
        for sink in self.addresses.values():
            sink.close()
        for sink in self.buildings.values():
            sink.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch Slovak location datasets and write separate reference, address, and building CSV files."
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "location",
        help="Directory where CSV files will be written",
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=None,
        help="Skip refresh when nuts3.csv is newer than this many days",
    )
    return parser.parse_args()


def effective_refresh_days(cli_value: int | None) -> int:
    if cli_value is not None:
        return cli_value
    return int(os.environ.get("LOCATION_DATA_DAYS_REFRESH", "30"))


def is_fresh(path: Path, refresh_days: int) -> bool:
    if refresh_days < 0 or not path.exists():
        return False
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds < refresh_days * 86400


def expected_files(out_dir: Path) -> list[Path]:
    files = [
        out_dir / "nuts3.csv",
        out_dir / "lau1.csv",
        out_dir / "lau2.csv",
        out_dir / "streets.csv",
    ]
    for abb in NAME_TO_ABB.values():
        files.append(out_dir / "addresses" / f"{abb}.csv")
        files.append(out_dir / "buildings" / f"{abb}.csv")
    return files


def has_existing_outputs(out_dir: Path) -> bool:
    return any(path.exists() for path in expected_files(out_dir))


def should_refresh(out_dir: Path, refresh_days: int) -> bool:
    files = expected_files(out_dir)
    if any(not p.exists() for p in files):
        return True
    return not is_fresh(out_dir / "nuts3.csv", refresh_days)


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
            resp.encoding = "utf-8"
            return resp.json()
        except (requests.exceptions.RequestException, ValueError) as exc:
            if attempt < tries - 1:
                time.sleep(backoff**attempt)
            else:
                print(f"[FAIL] {url} after {tries} tries: {exc}")
    return None


def parse_dataset_catalog(uri: str) -> list[str]:
    payload = fetch_json_with_retry(uri)
    if payload is None:
        return []

    graph = payload.get("@graph", [])
    for node in graph:
        datasets = node.get("dcat:dataset")
        if datasets is not None:
            return [
                item["iri"]
                for item in datasets
                if isinstance(item, dict) and "iri" in item
            ]
    return []


def basename(uri: str) -> str:
    return Path(urlparse(uri).path).name


def group_dataset_uris(uris: Iterable[str]) -> DatasetGroups:
    groups: dict[str, list[str]] = defaultdict(list)
    for uri in uris:
        name = basename(uri)
        if name == "nuts3.geojson":
            groups["nuts3"].append(uri)
        elif name == "lau1.geojson":
            groups["lau1"].append(uri)
        elif name == "lau2.geojson":
            groups["lau2"].append(uri)
        elif name.startswith("msa_by_lau1_"):
            groups["msa"].append(uri)
        elif name.startswith("maa_by_lau1_"):
            groups["maa"].append(uri)
        elif name.startswith("mba_by_lau2_"):
            groups["mba"].append(uri)
        else:
            groups["other"].append(uri)

    return DatasetGroups(
        nuts3=groups["nuts3"],
        lau1=groups["lau1"],
        lau2=groups["lau2"],
        msa=groups["msa"],
        maa=groups["maa"],
        mba=groups["mba"],
        other=groups["other"],
    )


def maybe_fix_mojibake(text: str) -> str:
    if not text:
        return text

    suspicious = ("Ă", "Ĺ", "Ä", "Ľ", "Ť", "â")
    if not any(ch in text for ch in suspicious):
        return text

    for source_encoding in ("cp1250", "latin1"):
        try:
            fixed = text.encode(source_encoding).decode("utf-8")
        except UnicodeError:
            continue

        old_bad = sum(text.count(ch) for ch in suspicious)
        new_bad = sum(fixed.count(ch) for ch in suspicious)
        if new_bad < old_bad:
            return fixed

    return text


def clean_str(value: Any) -> str:
    if value is None:
        return ""
    return maybe_fix_mojibake(str(value))


def coordinates_from_feature(feature: dict[str, Any]) -> tuple[str, str]:
    geometry = feature.get("geometry") or {}
    coords = geometry.get("coordinates") or []
    if len(coords) >= 2:
        return clean_str(coords[0]), clean_str(coords[1])
    return "", ""


def feature_properties(feature: dict[str, Any]) -> dict[str, Any]:
    props = feature.get("properties") or {}
    return props if isinstance(props, dict) else {}


def iter_features(payload: dict[str, Any]) -> Iterable[dict[str, Any]]:
    features = payload.get("features", [])
    if not isinstance(features, list):
        return []
    return (feature for feature in features if isinstance(feature, dict))


def staging_dir_for(out_dir: Path) -> Path:
    return out_dir.parent / f".{out_dir.name}.tmp"


def prepare_staging_dir(out_dir: Path) -> Path:
    staging_dir = staging_dir_for(out_dir)
    if staging_dir.exists():
        shutil.rmtree(staging_dir)
    staging_dir.mkdir(parents=True, exist_ok=True)
    return staging_dir


def remove_legacy_outputs(out_dir: Path) -> None:
    legacy_files = {
        out_dir / "kraje.csv",
        out_dir / "okresy.csv",
        out_dir / "obce.csv",
        out_dir / "ulice.csv",
        out_dir / "_UNKNOWN.csv",
        out_dir / "_PENDING_UNKNOWN.csv",
    }
    for abb in NAME_TO_ABB.values():
        legacy_files.add(out_dir / f"{abb}.csv")
        legacy_files.add(out_dir / f"{abb}_buildings.csv")

    for path in legacy_files:
        path.unlink(missing_ok=True)


def install_outputs(staging_dir: Path, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    for path in expected_files(out_dir):
        path.unlink(missing_ok=True)

    shutil.rmtree(out_dir / "addresses", ignore_errors=True)
    shutil.rmtree(out_dir / "buildings", ignore_errors=True)
    remove_legacy_outputs(out_dir)

    for src in staging_dir.iterdir():
        shutil.move(str(src), str(out_dir / src.name))


def cleanup_staging_dir(staging_dir: Path) -> None:
    if staging_dir.exists():
        shutil.rmtree(staging_dir)


def keep_existing_outputs_message(reason: str, out_dir: Path) -> int:
    print(f"Location refresh skipped: {reason}")
    print(f"Keeping existing outputs in {out_dir}")
    return 0


def process_nuts3(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_nuts3: dict[str, RegionInfo],
) -> None:
    seen_ids: set[tuple[str, str]] = set()
    for uri in uris:
        payload = fetch_json_with_retry(
            uri, session=session, tries=6, timeout=30.0, backoff=1.7
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            kraj_id = clean_str(props.get("nuts3_id"))
            kraj_name = clean_str(props.get("nuts3_name"))
            if not kraj_id or not kraj_name:
                continue
            region_by_nuts3[kraj_id] = RegionInfo(kraj_name=kraj_name, kraj_id=kraj_id)
            key = (kraj_id, clean_str(props.get("objectid")))
            if key in seen_ids:
                continue
            seen_ids.add(key)
            outputs.kraje.write(
                {
                    "Kraj": kraj_name,
                    "Kraj - ID": kraj_id,
                    "ID objektu": props.get("objectid"),
                    "IČO": props.get("ico"),
                    "Platné od": props.get("validfrom"),
                }
            )


def process_lau1(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_lau1: dict[str, RegionInfo],
    region_by_nuts3: dict[str, RegionInfo],
) -> None:
    seen_ids: set[tuple[str, str]] = set()
    for uri in uris:
        payload = fetch_json_with_retry(
            uri, session=session, tries=6, timeout=30.0, backoff=1.7
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            okres_id = clean_str(props.get("lau1_id"))
            okres_name = clean_str(props.get("lau1_name"))
            kraj_id = clean_str(props.get("nuts3_id"))
            kraj_name = clean_str(props.get("nuts3_name"))
            if not okres_id or not kraj_id:
                continue
            base_region = region_by_nuts3.get(kraj_id)
            if not kraj_name and base_region is not None:
                kraj_name = base_region.kraj_name
            info = RegionInfo(
                kraj_name=kraj_name,
                kraj_id=kraj_id,
                okres_name=okres_name,
                okres_id=okres_id,
            )
            region_by_lau1[okres_id] = info
            key = (okres_id, clean_str(props.get("objectid")))
            if key in seen_ids:
                continue
            seen_ids.add(key)
            outputs.okresy.write(
                {
                    "Okres": okres_name,
                    "Okres - ID": okres_id,
                    "Kraj": kraj_name,
                    "Kraj - ID": kraj_id,
                    "ID objektu": props.get("objectid"),
                    "IČO": props.get("ico"),
                    "Platné od": props.get("validfrom"),
                }
            )


def process_lau2(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_lau1: dict[str, RegionInfo],
    region_by_lau2: dict[str, RegionInfo],
) -> None:
    seen_ids: set[tuple[str, str]] = set()
    for uri in uris:
        payload = fetch_json_with_retry(
            uri, session=session, tries=6, timeout=30.0, backoff=1.7
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            obec_id = clean_str(props.get("lau2_id"))
            obec_name = clean_str(props.get("lau2_name"))
            okres_id = clean_str(props.get("lau1_id"))
            okres_info = region_by_lau1.get(okres_id)
            if not obec_id or okres_info is None:
                continue
            info = RegionInfo(
                kraj_name=okres_info.kraj_name,
                kraj_id=okres_info.kraj_id,
                okres_name=okres_info.okres_name,
                okres_id=okres_info.okres_id,
                obec_name=obec_name,
                obec_id=obec_id,
            )
            region_by_lau2[obec_id] = info
            key = (obec_id, clean_str(props.get("objectid")))
            if key in seen_ids:
                continue
            seen_ids.add(key)
            outputs.obce.write(
                {
                    "Obec": obec_name,
                    "Obec - ID": obec_id,
                    "Okres": info.okres_name,
                    "Okres - ID": info.okres_id,
                    "Kraj": info.kraj_name,
                    "Kraj - ID": info.kraj_id,
                    "ID objektu": props.get("objectid"),
                    "IČO": props.get("ico"),
                    "Platné od": props.get("validfrom"),
                }
            )


def process_msa(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_lau1: dict[str, RegionInfo],
) -> None:
    seen_rows: set[str] = set()
    for uri in tqdm(uris, desc="Fetching street datasets", unit="dataset"):
        payload = fetch_json_with_retry(
            uri, session=session, tries=6, timeout=30.0, backoff=1.7
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            street_id = clean_str(props.get("identifier"))
            street_name = clean_str(props.get("streetname"))

            okres_id = clean_str(props.get("lau1_id"))
            region = region_by_lau1.get(okres_id)
            if region is None:
                continue

            if street_id in seen_rows:
                continue
            seen_rows.add(street_id)
            outputs.ulice.write(
                {
                    "Ulica": street_name,
                    "Ulica - ID": street_id,
                    "Obec": props.get("lau2_name"),
                    "Obec - ID": props.get("lau2_id"),
                    "Okres": region.okres_name,
                    "Okres - ID": region.okres_id,
                    "Kraj": region.kraj_name,
                    "Kraj - ID": region.kraj_id,
                    "Platné od": props.get("validfrom"),
                }
            )


def process_mba(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_lau2: dict[str, RegionInfo],
) -> int:
    rows_written = 0

    for uri in tqdm(uris, desc="Fetching building datasets", unit="dataset"):
        payload = fetch_json_with_retry(
            uri, session=session, tries=6, timeout=30.0, backoff=1.7
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            obec_id = clean_str(props.get("lau2_id"))
            region = region_by_lau2.get(obec_id)
            if region is None:
                continue
            sink = outputs.buildings.get(region.kraj_name)
            if sink is None:
                continue

            sink.write(
                {
                    "ID budovy": props.get("identifier"),
                    "Typ budovy": props.get("buildingtypecodename"),
                    "Typ budovy - kód": props.get("buildingtypecode"),
                    "Účel budovy": props.get("buildingpurposename"),
                    "Účel budovy - kód": props.get("buildingpurposecode"),
                    "Súpisné číslo": props.get("propertyregistrationnumber"),
                    "Obec": region.obec_name,
                    "Obec - ID": region.obec_id,
                    "Okres": region.okres_name,
                    "Okres - ID": region.okres_id,
                    "Kraj": region.kraj_name,
                    "Kraj - ID": region.kraj_id,
                    "Platné od": props.get("validfrom"),
                }
            )
            rows_written += 1

    return rows_written


def process_maa(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
) -> int:
    rows_written = 0
    for uri in tqdm(uris, desc="Fetching address datasets", unit="dataset"):
        payload = fetch_json_with_retry(
            uri, session=session, tries=6, timeout=30.0, backoff=1.7
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            kraj_name = clean_str(props.get("nuts3_name"))
            sink = outputs.addresses.get(kraj_name)
            if sink is None:
                continue

            x, y = coordinates_from_feature(feature)
            sink.write(
                {
                    "ID budovy": props.get("identifier"),
                    "ID objektu": props.get("objectid"),
                    "Ulica": props.get("streetname"),
                    "Ulica - ID": props.get("street_id"),
                    "Súpisné číslo": props.get("propertyregistrationnumber"),
                    "Orientačné číslo": props.get("orientationnumber"),
                    "PSČ": props.get("postalcode"),
                    "Časť obce": props.get("district_name"),
                    "Časť obce - ID": props.get("district_id"),
                    "Obec": props.get("lau2_name"),
                    "Obec - ID": props.get("lau2_id"),
                    "Okres": props.get("lau1_name"),
                    "Okres - ID": props.get("lau1_id"),
                    "Kraj": kraj_name,
                    "Kraj - ID": props.get("nuts3_id"),
                    "Platné od": props.get("validfrom"),
                    "ADRBOD_X": x,
                    "ADRBOD_Y": y,
                    "URI": props.get("uri_identifier"),
                }
            )
            rows_written += 1
    return rows_written


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir
    refresh_days = effective_refresh_days(args.refresh_days)

    out_dir.mkdir(parents=True, exist_ok=True)

    if not should_refresh(out_dir, refresh_days):
        print(
            f"Skipping location refresh: all expected files exist and are newer than {refresh_days} days"
        )
        return 0

    print("Obtaining dataset catalog...")
    uris = parse_dataset_catalog(URI_ROOT)
    if not uris:
        if has_existing_outputs(out_dir):
            return keep_existing_outputs_message(
                f"failed to obtain dataset catalog from {URI_ROOT}", out_dir
            )
        print(f"Location refresh failed: could not obtain dataset catalog from {URI_ROOT}")
        return 1

    groups = group_dataset_uris(uris)
    if groups.other:
        print(f"Warning: found {len(groups.other)} unclassified dataset URIs")
        for uri in groups.other[:10]:
            print(f"  - {uri}")
        if len(groups.other) > 10:
            print("  ...")

    staging_dir = prepare_staging_dir(out_dir)

    try:
        outputs = OutputManager(staging_dir)
        region_by_nuts3: dict[str, RegionInfo] = {}
        region_by_lau1: dict[str, RegionInfo] = {}
        region_by_lau2: dict[str, RegionInfo] = {}
        try:
            session = requests.Session()
            process_nuts3(session, groups.nuts3, outputs, region_by_nuts3)
            process_lau1(session, groups.lau1, outputs, region_by_lau1, region_by_nuts3)
            process_lau2(session, groups.lau2, outputs, region_by_lau1, region_by_lau2)
            process_msa(session, groups.msa, outputs, region_by_lau1)
            building_rows = process_mba(
                session,
                groups.mba,
                outputs,
                region_by_lau2,
            )
            address_rows = process_maa(
                session,
                groups.maa,
                outputs,
            )
        finally:
            outputs.close()

        if address_rows == 0 and building_rows == 0:
            if has_existing_outputs(out_dir):
                return keep_existing_outputs_message(
                    "catalog was fetched but no address/building rows were produced",
                    out_dir,
                )
            print(
                "Location refresh failed: catalog was fetched but no address/building rows were produced"
            )
            return 1

        install_outputs(staging_dir, out_dir)
        print(
            f"Wrote {address_rows} address rows, {building_rows} building rows, "
            f"{len(region_by_nuts3)} kraje, {len(region_by_lau1)} okresy, {len(region_by_lau2)} obce"
        )
        return 0
    finally:
        cleanup_staging_dir(staging_dir)


if __name__ == "__main__":
    raise SystemExit(main())
