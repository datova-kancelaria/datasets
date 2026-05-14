from __future__ import annotations

import sys
import json
import argparse
import csv
import os
import re
import shutil
import time
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable
from urllib.parse import urlparse

import requests
from tqdm import tqdm


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from tables import write_xlsx_for_csv_tree

URI_ROOT = "https://rageo.minv.sk/opendata/katalog.json"

CONNECT_TIMEOUT = 15.0
CATALOG_TIMEOUT = (CONNECT_TIMEOUT, 120.0)
METADATA_TIMEOUT = (CONNECT_TIMEOUT, 60.0)
GEOJSON_TIMEOUT = (CONNECT_TIMEOUT, 600.0)  # 10 minutes

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

DISTRICTS_HEADER = [
    "Časť obce",
    "Časť obce - ID",
    "Obec",
    "Obec - ID",
    "Okres",
    "Okres - ID",
    "Kraj",
    "Kraj - ID",
    "Platné od",
]

ULICE_HEADER = [
    "Ulica",
    "Ulica - ID",
    "Časť obce",
    "Časť obce - ID",
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
    "Časť obce",
    "Časť obce - ID",
    "Obec",
    "Obec - ID",
    "Okres",
    "Okres - ID",
    "Kraj",
    "Kraj - ID",
    "Platné od",
]


STREET_DISTRICT_AUDIT_HEADER = [
    "Ulica - ID",
    "Ulica",
    "Obec - ID",
    "Obec",
    "Okres - ID",
    "Okres",
    "Kraj - ID",
    "Kraj",
    "Prvý district_id",
    "Prvý district_name",
    "Konfliktné district_id hodnoty",
    "Konfliktné district_name hodnoty",
]

STREET_CONTEXT_AUDIT_HEADER = [
    "Ulica - ID",
    "Ulica",
    "Obec - ID",
    "Obec",
    "Okres - ID",
    "Okres",
    "Kraj - ID",
    "Kraj",
    "Konfliktné polia",
]

DISTRICT_AUDIT_HEADER = [
    "Časť obce - ID",
    "Časť obce",
    "Obec - ID",
    "Obec",
    "Okres - ID",
    "Okres",
    "Kraj - ID",
    "Kraj",
    "Konfliktné polia",
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
    streets: list[str]
    addresses: list[str]
    buildings: list[str]
    other: list[str]


@dataclass
class StreetLink:
    street_id: str
    street_name: str = ""
    district_id: str = ""
    district_name: str = ""
    obec_id: str = ""
    obec_name: str = ""
    okres_id: str = ""
    okres_name: str = ""
    kraj_id: str = ""
    kraj_name: str = ""
    validfrom: str = ""
    district_ambiguous: bool = False
    context_conflict: bool = False
    district_ids: set[str] = field(default_factory=set)
    district_names: set[str] = field(default_factory=set)


@dataclass
class DistrictRecord:
    district_id: str
    district_name: str = ""
    obec_id: str = ""
    obec_name: str = ""
    okres_id: str = ""
    okres_name: str = ""
    kraj_id: str = ""
    kraj_name: str = ""
    validfrom: str = ""
    conflict: bool = False


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
        self.districts = CsvSink(out_dir / "districts.csv", DISTRICTS_HEADER)
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
        self.districts.close()
        self.ulice.close()
        for sink in self.addresses.values():
            sink.close()
        for sink in self.buildings.values():
            sink.close()


@dataclass
class LinkStats:
    street_district_conflicts: int = 0
    street_context_conflicts: int = 0
    district_conflicts: int = 0


@dataclass
class LinkAudit:
    street_district_rows: dict[str, dict[str, str]]
    street_context_rows: dict[str, dict[str, str]]
    district_rows: dict[str, dict[str, str]]

    def __init__(self) -> None:
        self.street_district_rows = {}
        self.street_context_rows = {}
        self.district_rows = {}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch Slovak location datasets and write separate reference, address, "
            "building, district, and street CSV/XLSX files."
        )
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "location",
        help="Directory where files will be written",
    )
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=None,
        help="Skip refresh when nuts3.csv is newer than this many days",
    )
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Write downloaded source JSON/GeoJSON payloads under <out-dir>/debug",
    )
    parser.add_argument(
        "--skip-excel",
        "--skip-xlsx",
        dest="write_xlsx",
        action="store_false",
        default=True,
        help="Do not create XLSX copies of generated CSV files",
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


def expected_csv_files(out_dir: Path) -> list[Path]:
    files = [
        out_dir / "nuts3.csv",
        out_dir / "lau1.csv",
        out_dir / "lau2.csv",
        out_dir / "districts.csv",
        out_dir / "streets.csv",
    ]
    for abb in NAME_TO_ABB.values():
        files.append(out_dir / "addresses" / f"{abb}.csv")
        files.append(out_dir / "buildings" / f"{abb}.csv")
    return files


def expected_xlsx_files(out_dir: Path) -> list[Path]:
    return [path.with_suffix(".xlsx") for path in expected_csv_files(out_dir)]


def expected_files(out_dir: Path, *, include_xlsx: bool = True) -> list[Path]:
    files = expected_csv_files(out_dir)
    if include_xlsx:
        files += expected_xlsx_files(out_dir)
    return files


def has_existing_outputs(out_dir: Path) -> bool:
    return any(path.exists() for path in expected_files(out_dir, include_xlsx=True))


def should_refresh(out_dir: Path, refresh_days: int, *, include_xlsx: bool = True) -> bool:
    files = expected_files(out_dir, include_xlsx=include_xlsx)
    if any(not p.exists() for p in files):
        return True
    return not is_fresh(out_dir / "nuts3.csv", refresh_days)


def safe_debug_filename(url: str) -> str:
    path = Path(urlparse(url).path)
    name = path.name or "index.json"

    # Keep useful original names, but avoid weird filesystem characters.
    safe = "".join(ch if ch.isalnum() or ch in "._-" else "_" for ch in name)
    return safe or "payload.json"


def dump_debug_json(
    payload: dict[str, Any],
    *,
    url: str,
    debug_dir: Path | None,
    category: str,
) -> None:
    if debug_dir is None:
        return

    target_dir = debug_dir / category
    target_dir.mkdir(parents=True, exist_ok=True)

    target = target_dir / safe_debug_filename(url)

    # Avoid accidental overwrite if two URLs have the same basename.
    if target.exists():
        stem = target.stem
        suffix = target.suffix or ".json"
        i = 2
        while True:
            candidate = target_dir / f"{stem}_{i}{suffix}"
            if not candidate.exists():
                target = candidate
                break
            i += 1

    with target.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def fetch_json_with_retry(
    url: str,
    *,
    session: requests.Session | None = None,
    tries: int = 5,
    timeout: float | tuple[float, float] = 30.0,
    backoff: float = 1.5,
    debug_dir: Path | None = None,
    debug_category: str = "json",
) -> dict[str, Any] | None:
    s = session or requests
    for attempt in range(tries):
        try:
            resp = s.get(url, timeout=timeout)
            resp.raise_for_status()
            resp.encoding = "utf-8"
            payload = resp.json()
            if isinstance(payload, dict):
                dump_debug_json(
                    payload,
                    url=url,
                    debug_dir=debug_dir,
                    category=debug_category,
                )
                return payload
            return payload
        except (requests.exceptions.RequestException, ValueError) as exc:
            if attempt < tries - 1:
                time.sleep(backoff**attempt)
            else:
                print(f"[FAIL] {url} after {tries} tries: {exc}")
    return None


def parse_dataset_catalog(
    uri: str,
    *,
    session: requests.Session | None = None,
    debug_dir: Path | None = None,
) -> list[str]:
    payload = fetch_json_with_retry(
        uri,
        session=session,
        timeout=CATALOG_TIMEOUT,
        debug_dir=debug_dir,
        debug_category="catalog",
    )
    if payload is None:
        return []

    graph = payload.get("@graph", [])
    if not isinstance(graph, list):
        return []

    for node in graph:
        if not isinstance(node, dict):
            continue

        datasets = node.get("dcat:dataset")
        if datasets is not None:
            return iri_values(datasets)

    return []


def basename(uri: str) -> str:
    return Path(urlparse(uri).path).name


def iri_values(value: Any) -> list[str]:
    if isinstance(value, dict):
        iri = clean_str(value.get("iri") or value.get("@id"))
        return [iri] if iri else []

    if isinstance(value, list):
        out: list[str] = []
        for item in value:
            out.extend(iri_values(item))
        return out

    return []


def distribution_url_from_metadata(payload: dict[str, Any]) -> str:
    graph = payload.get("@graph", [])
    if not isinstance(graph, list):
        return ""

    for node in graph:
        if not isinstance(node, dict):
            continue

        node_type = node.get("@type")
        is_distribution = (
            node_type == "dcat:Distribution"
            or (isinstance(node_type, list) and "dcat:Distribution" in node_type)
        )
        if not is_distribution:
            continue

        for key in ("dcat:downloadURL", "dcat:accessURL"):
            urls = iri_values(node.get(key))
            if urls:
                return urls[0]

    return ""


def resolve_dataset_uri(
    uri: str,
    *,
    session: requests.Session | None = None,
    debug_dir: Path | None = None,
) -> str:
    # Backward compatibility: old catalog may already point directly to GeoJSON.
    if basename(uri).endswith(".geojson"):
        return uri

    payload = fetch_json_with_retry(
        uri,
        session=session,
        tries=5,
        timeout=METADATA_TIMEOUT,
        backoff=1.7,
        debug_dir=debug_dir,
        debug_category="metadata",
    )
    if payload is None:
        return ""

    geojson_uri = distribution_url_from_metadata(payload)
    if not geojson_uri:
        print(f"[WARN] metadata has no GeoJSON distribution URL: {uri}")
        return ""

    return geojson_uri


def resolve_dataset_uris(
    uris: Iterable[str],
    *,
    session: requests.Session | None = None,
    debug_dir: Path | None = None,
) -> list[str]:
    resolved: list[str] = []

    for uri in tqdm(list(uris), desc="Resolving dataset metadata", unit="dataset"):
        resolved_uri = resolve_dataset_uri(
            uri,
            session=session,
            debug_dir=debug_dir,
        )
        if resolved_uri:
            resolved.append(resolved_uri)

    return resolved


def group_dataset_uris(uris: Iterable[str]) -> DatasetGroups:
    groups: dict[str, list[str]] = defaultdict(list)

    skipped_aggregate: dict[str, int] = defaultdict(int)

    for uri in uris:
        name = basename(uri)

        if name == "nuts3.geojson":
            groups["nuts3"].append(uri)
        elif name == "lau1.geojson":
            groups["lau1"].append(uri)
        elif name == "lau2.geojson":
            groups["lau2"].append(uri) # these seem to be removed from the catalogue

        elif name.startswith("address_by_lau2_"):
            groups["addresses"].append(uri)
        elif name.startswith("address_by_lau1_") or name.startswith("address_by_nuts3_"):
            skipped_aggregate["addresses"] += 1

        elif name.startswith("street_by_lau2_"):
            groups["streets"].append(uri)
        elif name.startswith("street_by_lau1_") or name.startswith("street_by_nuts3_"):
            skipped_aggregate["streets"] += 1

        elif name.startswith("building_by_lau2_"):
            groups["buildings"].append(uri)
        elif name.startswith("building_by_lau1_") or name.startswith("building_by_nuts3_"):
            skipped_aggregate["buildings"] += 1

        elif name.startswith("maa_by_lau2_"): # fallback to old naming style
            groups["addresses"].append(uri)
        elif name.startswith("msa_by_lau2_"):
            groups["streets"].append(uri)
        elif name.startswith("mba_by_lau2_"):
            groups["buildings"].append(uri)

        elif name.startswith("maa_by_"):
            skipped_aggregate["addresses"] += 1
        elif name.startswith("msa_by_"):
            skipped_aggregate["streets"] += 1
        elif name.startswith("mba_by_"):
            skipped_aggregate["buildings"] += 1

        else:
            groups["other"].append(uri)

    if skipped_aggregate:
        print(
            "Skipped aggregate duplicate datasets: "
            + ", ".join(
                f"{kind}={count}"
                for kind, count in sorted(skipped_aggregate.items())
            )
        )

    return DatasetGroups(
        nuts3=groups["nuts3"],
        lau1=groups["lau1"],
        lau2=groups["lau2"],
        streets=groups["streets"],
        addresses=groups["addresses"],
        buildings=groups["buildings"],
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

    for path in expected_files(out_dir, include_xlsx=True):
        path.unlink(missing_ok=True)

    shutil.rmtree(out_dir / "addresses", ignore_errors=True)
    shutil.rmtree(out_dir / "buildings", ignore_errors=True)
    shutil.rmtree(out_dir / "audit", ignore_errors=True)
    shutil.rmtree(out_dir / "debug", ignore_errors=True)
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


def min_date(current: str, new_value: str) -> str:
    current = clean_str(current)
    new_value = clean_str(new_value)
    if not current:
        return new_value
    if not new_value:
        return current
    return min(current, new_value)


def merge_prefer_existing(current: str, new_value: str) -> tuple[str, bool]:
    current = clean_str(current)
    new_value = clean_str(new_value)
    if not new_value:
        return current, False
    if not current:
        return new_value, False
    if current == new_value:
        return current, False
    return current, True


def add_pipe_value(current: str, value: str) -> str:
    current = clean_str(current)
    value = clean_str(value)
    if not value:
        return current
    parts = [part.strip() for part in current.split(" | ") if part.strip()] if current else []
    if value not in parts:
        parts.append(value)
    return " | ".join(parts)




def sorted_unique_values(values: Iterable[Any]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        clean = clean_str(value).strip()
        if not clean or clean in seen:
            continue
        seen.add(clean)
        out.append(clean)
    return sorted(out, key=sort_text)


def join_csv_values(values: Iterable[Any]) -> str:
    return ", ".join(sorted_unique_values(values))

def record_street_district_conflict(
    audit: LinkAudit,
    *,
    street_id: str,
    street_name: str,
    obec_id: str,
    obec_name: str,
    okres_id: str,
    okres_name: str,
    kraj_id: str,
    kraj_name: str,
    first_district_id: str,
    first_district_name: str,
    new_district_id: str,
    new_district_name: str,
) -> None:
    row = audit.street_district_rows.setdefault(
        street_id,
        {
            "Ulica - ID": clean_str(street_id),
            "Ulica": clean_str(street_name),
            "Obec - ID": clean_str(obec_id),
            "Obec": clean_str(obec_name),
            "Okres - ID": clean_str(okres_id),
            "Okres": clean_str(okres_name),
            "Kraj - ID": clean_str(kraj_id),
            "Kraj": clean_str(kraj_name),
            "Prvý district_id": clean_str(first_district_id),
            "Prvý district_name": clean_str(first_district_name),
            "Konfliktné district_id hodnoty": "",
            "Konfliktné district_name hodnoty": "",
        },
    )
    for key, value in (
        ("Ulica", street_name),
        ("Obec - ID", obec_id),
        ("Obec", obec_name),
        ("Okres - ID", okres_id),
        ("Okres", okres_name),
        ("Kraj - ID", kraj_id),
        ("Kraj", kraj_name),
    ):
        row[key] = row[key] or clean_str(value)
    row["Konfliktné district_id hodnoty"] = add_pipe_value(
        row["Konfliktné district_id hodnoty"], new_district_id
    )
    row["Konfliktné district_name hodnoty"] = add_pipe_value(
        row["Konfliktné district_name hodnoty"], new_district_name
    )


def record_street_context_conflict(
    audit: LinkAudit,
    *,
    street_id: str,
    street_name: str,
    obec_id: str,
    obec_name: str,
    okres_id: str,
    okres_name: str,
    kraj_id: str,
    kraj_name: str,
    field_name: str,
) -> None:
    row = audit.street_context_rows.setdefault(
        street_id,
        {
            "Ulica - ID": clean_str(street_id),
            "Ulica": clean_str(street_name),
            "Obec - ID": clean_str(obec_id),
            "Obec": clean_str(obec_name),
            "Okres - ID": clean_str(okres_id),
            "Okres": clean_str(okres_name),
            "Kraj - ID": clean_str(kraj_id),
            "Kraj": clean_str(kraj_name),
            "Konfliktné polia": "",
        },
    )
    row["Konfliktné polia"] = add_pipe_value(row["Konfliktné polia"], field_name)


def record_district_conflict(
    audit: LinkAudit,
    *,
    district_id: str,
    district_name: str,
    obec_id: str,
    obec_name: str,
    okres_id: str,
    okres_name: str,
    kraj_id: str,
    kraj_name: str,
    field_name: str,
) -> None:
    row = audit.district_rows.setdefault(
        district_id,
        {
            "Časť obce - ID": clean_str(district_id),
            "Časť obce": clean_str(district_name),
            "Obec - ID": clean_str(obec_id),
            "Obec": clean_str(obec_name),
            "Okres - ID": clean_str(okres_id),
            "Okres": clean_str(okres_name),
            "Kraj - ID": clean_str(kraj_id),
            "Kraj": clean_str(kraj_name),
            "Konfliktné polia": "",
        },
    )
    for key, value in (
        ("Časť obce", district_name),
        ("Obec - ID", obec_id),
        ("Obec", obec_name),
        ("Okres - ID", okres_id),
        ("Okres", okres_name),
        ("Kraj - ID", kraj_id),
        ("Kraj", kraj_name),
    ):
        row[key] = row[key] or clean_str(value)
    row["Konfliktné polia"] = add_pipe_value(row["Konfliktné polia"], field_name)


def update_street_link(
    street_links: dict[str, StreetLink],
    audit: LinkAudit,
    *,
    street_id: str,
    street_name: str,
    district_id: str,
    district_name: str,
    obec_id: str,
    obec_name: str,
    okres_id: str,
    okres_name: str,
    kraj_id: str,
    kraj_name: str,
    validfrom: str,
    stats: LinkStats,
) -> None:
    street_id = clean_str(street_id)
    if not street_id:
        return

    normalized_district_id = clean_str(district_id)
    normalized_district_name = clean_str(district_name)

    entry = street_links.get(street_id)
    if entry is None:
        street_links[street_id] = StreetLink(
            street_id=street_id,
            street_name=clean_str(street_name),
            district_id=normalized_district_id,
            district_name=normalized_district_name,
            obec_id=clean_str(obec_id),
            obec_name=clean_str(obec_name),
            okres_id=clean_str(okres_id),
            okres_name=clean_str(okres_name),
            kraj_id=clean_str(kraj_id),
            kraj_name=clean_str(kraj_name),
            validfrom=clean_str(validfrom),
            district_ids={normalized_district_id} if normalized_district_id else set(),
            district_names={normalized_district_name} if normalized_district_name else set(),
        )
        return

    if normalized_district_id:
        if entry.district_id and entry.district_id != normalized_district_id:
            record_street_district_conflict(
                audit,
                street_id=street_id,
                street_name=street_name or entry.street_name,
                obec_id=obec_id or entry.obec_id,
                obec_name=obec_name or entry.obec_name,
                okres_id=okres_id or entry.okres_id,
                okres_name=okres_name or entry.okres_name,
                kraj_id=kraj_id or entry.kraj_id,
                kraj_name=kraj_name or entry.kraj_name,
                first_district_id=entry.district_id,
                first_district_name=entry.district_name,
                new_district_id=normalized_district_id,
                new_district_name=normalized_district_name,
            )
            if not entry.district_ambiguous:
                entry.district_ambiguous = True
                stats.street_district_conflicts += 1
        elif not entry.district_id:
            entry.district_id = normalized_district_id
        entry.district_ids.add(normalized_district_id)

    if normalized_district_name:
        if entry.district_name and entry.district_name != normalized_district_name:
            if entry.district_id and (not normalized_district_id or normalized_district_id == entry.district_id):
                record_street_district_conflict(
                    audit,
                    street_id=street_id,
                    street_name=street_name or entry.street_name,
                    obec_id=obec_id or entry.obec_id,
                    obec_name=obec_name or entry.obec_name,
                    okres_id=okres_id or entry.okres_id,
                    okres_name=okres_name or entry.okres_name,
                    kraj_id=kraj_id or entry.kraj_id,
                    kraj_name=kraj_name or entry.kraj_name,
                    first_district_id=entry.district_id,
                    first_district_name=entry.district_name,
                    new_district_id=normalized_district_id,
                    new_district_name=normalized_district_name,
                )
                if not entry.district_ambiguous:
                    entry.district_ambiguous = True
                    stats.street_district_conflicts += 1
        elif not entry.district_name:
            entry.district_name = normalized_district_name
        entry.district_names.add(normalized_district_name)

    for attr, new_value in (
        ("street_name", street_name),
        ("obec_id", obec_id),
        ("obec_name", obec_name),
        ("okres_id", okres_id),
        ("okres_name", okres_name),
        ("kraj_id", kraj_id),
        ("kraj_name", kraj_name),
    ):
        merged, conflict = merge_prefer_existing(getattr(entry, attr), clean_str(new_value))
        setattr(entry, attr, merged)
        if conflict:
            record_street_context_conflict(
                audit,
                street_id=street_id,
                street_name=entry.street_name or street_name,
                obec_id=entry.obec_id or obec_id,
                obec_name=entry.obec_name or obec_name,
                okres_id=entry.okres_id or okres_id,
                okres_name=entry.okres_name or okres_name,
                kraj_id=entry.kraj_id or kraj_id,
                kraj_name=entry.kraj_name or kraj_name,
                field_name=attr,
            )
            if not entry.context_conflict:
                entry.context_conflict = True
                stats.street_context_conflicts += 1

    entry.validfrom = min_date(entry.validfrom, validfrom)


def register_district(
    districts_by_id: dict[str, DistrictRecord],
    audit: LinkAudit,
    *,
    district_id: str,
    district_name: str,
    obec_id: str,
    obec_name: str,
    okres_id: str,
    okres_name: str,
    kraj_id: str,
    kraj_name: str,
    validfrom: str,
    stats: LinkStats,
) -> None:
    district_id = clean_str(district_id)
    if not district_id:
        return

    entry = districts_by_id.get(district_id)
    if entry is None:
        districts_by_id[district_id] = DistrictRecord(
            district_id=district_id,
            district_name=clean_str(district_name),
            obec_id=clean_str(obec_id),
            obec_name=clean_str(obec_name),
            okres_id=clean_str(okres_id),
            okres_name=clean_str(okres_name),
            kraj_id=clean_str(kraj_id),
            kraj_name=clean_str(kraj_name),
            validfrom=clean_str(validfrom),
        )
        return

    had_conflict = entry.conflict
    for attr, new_value in (
        ("district_name", district_name),
        ("obec_id", obec_id),
        ("obec_name", obec_name),
        ("okres_id", okres_id),
        ("okres_name", okres_name),
        ("kraj_id", kraj_id),
        ("kraj_name", kraj_name),
    ):
        merged, conflict = merge_prefer_existing(getattr(entry, attr), clean_str(new_value))
        setattr(entry, attr, merged)
        if conflict:
            record_district_conflict(
                audit,
                district_id=district_id,
                district_name=entry.district_name or district_name,
                obec_id=entry.obec_id or obec_id,
                obec_name=entry.obec_name or obec_name,
                okres_id=entry.okres_id or okres_id,
                okres_name=entry.okres_name or okres_name,
                kraj_id=entry.kraj_id or kraj_id,
                kraj_name=entry.kraj_name or kraj_name,
                field_name=attr,
            )
            entry.conflict = True

    entry.validfrom = min_date(entry.validfrom, validfrom)
    if entry.conflict and not had_conflict:
        stats.district_conflicts += 1


def register_region_from_props(
    props: dict[str, Any],
    outputs: OutputManager,
    region_by_nuts3: dict[str, RegionInfo],
    region_by_lau1: dict[str, RegionInfo],
    region_by_lau2: dict[str, RegionInfo],
    seen_nuts3: set[str],
    seen_lau1: set[str],
    seen_lau2: set[str],
) -> None:
    kraj_id = clean_str(props.get("nuts3_id"))
    kraj_name = clean_str(props.get("nuts3_name"))
    okres_id = clean_str(props.get("lau1_id"))
    okres_name = clean_str(props.get("lau1_name"))
    obec_id = clean_str(props.get("lau2_id"))
    obec_name = clean_str(props.get("lau2_name"))

    if kraj_id and kraj_name:
        region_by_nuts3[kraj_id] = RegionInfo(kraj_name=kraj_name, kraj_id=kraj_id)

        if kraj_id not in seen_nuts3:
            seen_nuts3.add(kraj_id)
            outputs.kraje.write({
                "Kraj": kraj_name,
                "Kraj - ID": kraj_id,
                "ID objektu": "",
                "IČO": "",
                "Platné od": "",
            })

    if okres_id and okres_name and kraj_id and kraj_name:
        region_by_lau1[okres_id] = RegionInfo(
            kraj_name=kraj_name,
            kraj_id=kraj_id,
            okres_name=okres_name,
            okres_id=okres_id,
        )

        if okres_id not in seen_lau1:
            seen_lau1.add(okres_id)
            outputs.okresy.write({
                "Okres": okres_name,
                "Okres - ID": okres_id,
                "Kraj": kraj_name,
                "Kraj - ID": kraj_id,
                "ID objektu": "",
                "IČO": "",
                "Platné od": "",
            })

    if obec_id and obec_name and okres_id and okres_name and kraj_id and kraj_name:
        region_by_lau2[obec_id] = RegionInfo(
            kraj_name=kraj_name,
            kraj_id=kraj_id,
            okres_name=okres_name,
            okres_id=okres_id,
            obec_name=obec_name,
            obec_id=obec_id,
        )

        if obec_id not in seen_lau2:
            seen_lau2.add(obec_id)
            outputs.obce.write({
                "Obec": obec_name,
                "Obec - ID": obec_id,
                "Okres": okres_name,
                "Okres - ID": okres_id,
                "Kraj": kraj_name,
                "Kraj - ID": kraj_id,
                "ID objektu": "",
                "IČO": "",
                "Platné od": "",
            })



def process_nuts3(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_nuts3: dict[str, RegionInfo],
    debug_dir: Path | None = None,
) -> None:
    seen_ids: set[tuple[str, str]] = set()
    for uri in uris:
        payload = fetch_json_with_retry(
            uri,
            session=session,
            tries=6,
            timeout=GEOJSON_TIMEOUT,
            backoff=1.7,
            debug_dir=debug_dir,
            debug_category="geojson",
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
    debug_dir: Path | None = None,
) -> None:
    seen_ids: set[tuple[str, str]] = set()
    for uri in uris:
        payload = fetch_json_with_retry(
            uri,
            session=session,
            tries=6,
            timeout=GEOJSON_TIMEOUT,
            backoff=1.7,
            debug_dir=debug_dir,
            debug_category="geojson",
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
    debug_dir: Path | None = None,
) -> None:
    seen_ids: set[tuple[str, str]] = set()
    for uri in uris:
        payload = fetch_json_with_retry(
            uri,
            session=session,
            tries=6,
            timeout=GEOJSON_TIMEOUT,
            backoff=1.7,
            debug_dir=debug_dir,
            debug_category="geojson",
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


def process_maa(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    street_links: dict[str, StreetLink],
    districts_by_id: dict[str, DistrictRecord],
    stats: LinkStats,
    audit: LinkAudit,
    region_by_nuts3: dict[str, RegionInfo],
    region_by_lau1: dict[str, RegionInfo],
    region_by_lau2: dict[str, RegionInfo],
    seen_nuts3: set[str],
    seen_lau1: set[str],
    seen_lau2: set[str],
    debug_dir: Path | None = None,
) -> int:
    rows_written = 0
    for uri in tqdm(uris, desc="Fetching address datasets", unit="dataset"):
        payload = fetch_json_with_retry(
            uri,
            session=session,
            tries=6,
            timeout=GEOJSON_TIMEOUT,
            backoff=1.7,
            debug_dir=debug_dir,
            debug_category="geojson",
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            register_region_from_props(
                props,
                outputs,
                region_by_nuts3,
                region_by_lau1,
                region_by_lau2,
                seen_nuts3,
                seen_lau1,
                seen_lau2,
            )
            kraj_name = clean_str(props.get("nuts3_name"))
            sink = outputs.addresses.get(kraj_name)
            if sink is None:
                continue

            street_id = clean_str(props.get("street_id"))
            district_id = clean_str(props.get("district_id"))
            district_name = clean_str(props.get("district_name"))
            obec_id = clean_str(props.get("lau2_id"))
            obec_name = clean_str(props.get("lau2_name"))
            okres_id = clean_str(props.get("lau1_id"))
            okres_name = clean_str(props.get("lau1_name"))
            kraj_id = clean_str(props.get("nuts3_id"))
            validfrom = clean_str(props.get("validfrom"))

            update_street_link(
                street_links,
                audit,
                street_id=street_id,
                street_name=clean_str(props.get("streetname")),
                district_id=district_id,
                district_name=district_name,
                obec_id=obec_id,
                obec_name=obec_name,
                okres_id=okres_id,
                okres_name=okres_name,
                kraj_id=kraj_id,
                kraj_name=kraj_name,
                validfrom=validfrom,
                stats=stats,
            )
            register_district(
                districts_by_id,
                audit,
                district_id=district_id,
                district_name=district_name,
                obec_id=obec_id,
                obec_name=obec_name,
                okres_id=okres_id,
                okres_name=okres_name,
                kraj_id=kraj_id,
                kraj_name=kraj_name,
                validfrom=validfrom,
                stats=stats,
            )

            x, y = coordinates_from_feature(feature)
            sink.write(
                {
                    "ID budovy": props.get("identifier"),
                    "ID objektu": props.get("objectid"),
                    "Ulica": props.get("streetname"),
                    "Ulica - ID": street_id,
                    "Súpisné číslo": props.get("propertyregistrationnumber"),
                    "Orientačné číslo": props.get("orientationnumber"),
                    "PSČ": props.get("postalcode"),
                    "Časť obce": district_name,
                    "Časť obce - ID": district_id,
                    "Obec": obec_name,
                    "Obec - ID": obec_id,
                    "Okres": okres_name,
                    "Okres - ID": okres_id,
                    "Kraj": kraj_name,
                    "Kraj - ID": kraj_id,
                    "Platné od": validfrom,
                    "ADRBOD_X": x,
                    "ADRBOD_Y": y,
                    "URI": props.get("uri_identifier"),
                }
            )
            rows_written += 1
    return rows_written


def process_msa(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_lau1: dict[str, RegionInfo],
    street_links: dict[str, StreetLink],
    debug_dir: Path | None = None,
) -> None:
    rows_by_street: dict[str, dict[str, Any]] = {}
    direct_district_ids: dict[str, set[str]] = defaultdict(set)
    direct_district_names: dict[str, set[str]] = defaultdict(set)

    for uri in tqdm(uris, desc="Fetching street datasets", unit="dataset"):
        payload = fetch_json_with_retry(
            uri,
            session=session,
            tries=6,
            timeout=GEOJSON_TIMEOUT,
            backoff=1.7,
            debug_dir=debug_dir,
            debug_category="geojson",
        )
        if payload is None:
            continue
        for feature in iter_features(payload):
            props = feature_properties(feature)
            street_id = clean_str(props.get("identifier"))
            street_name = clean_str(props.get("streetname"))
            okres_id = clean_str(props.get("lau1_id"))
            region = region_by_lau1.get(okres_id)
            if region is None or not street_id:
                continue

            direct_district_id = clean_str(props.get("district_id"))
            direct_district_name = clean_str(props.get("district_name"))
            if direct_district_id:
                direct_district_ids[street_id].add(direct_district_id)
            if direct_district_name:
                direct_district_names[street_id].add(direct_district_name)

            row = rows_by_street.get(street_id)
            if row is None:
                rows_by_street[street_id] = {
                    "Ulica": street_name,
                    "Ulica - ID": street_id,
                    "Časť obce": "",
                    "Časť obce - ID": "",
                    "Obec": props.get("lau2_name"),
                    "Obec - ID": props.get("lau2_id"),
                    "Okres": region.okres_name,
                    "Okres - ID": region.okres_id,
                    "Kraj": region.kraj_name,
                    "Kraj - ID": region.kraj_id,
                    "Platné od": props.get("validfrom"),
                }
            else:
                row["Platné od"] = min_date(row.get("Platné od", ""), props.get("validfrom"))

    for street_id, row in rows_by_street.items():
        inferred = street_links.get(street_id)
        district_ids = set(direct_district_ids.get(street_id, set()))
        district_names = set(direct_district_names.get(street_id, set()))
        if inferred is not None:
            district_ids.update(inferred.district_ids)
            district_names.update(inferred.district_names)
            if not district_ids and inferred.district_id:
                district_ids.add(inferred.district_id)
            if not district_names and inferred.district_name:
                district_names.add(inferred.district_name)

        row["Časť obce - ID"] = join_csv_values(district_ids)
        row["Časť obce"] = join_csv_values(district_names)
        outputs.ulice.write(row)


def process_mba(
    session: requests.Session,
    uris: list[str],
    outputs: OutputManager,
    region_by_lau2: dict[str, RegionInfo],
    districts_by_id: dict[str, DistrictRecord],
    stats: LinkStats,
    audit: LinkAudit,
    debug_dir: Path | None = None,
) -> int:
    rows_written = 0

    for uri in tqdm(uris, desc="Fetching building datasets", unit="dataset"):
        payload = fetch_json_with_retry(
            uri,
            session=session,
            tries=6,
            timeout=GEOJSON_TIMEOUT,
            backoff=1.7,
            debug_dir=debug_dir,
            debug_category="geojson",
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

            district_name = clean_str(props.get("district_name"))
            district_id = clean_str(props.get("district_id"))
            validfrom = clean_str(props.get("validfrom"))

            register_district(
                districts_by_id,
                audit,
                district_id=district_id,
                district_name=district_name,
                obec_id=region.obec_id,
                obec_name=region.obec_name,
                okres_id=region.okres_id,
                okres_name=region.okres_name,
                kraj_id=region.kraj_id,
                kraj_name=region.kraj_name,
                validfrom=validfrom,
                stats=stats,
            )

            sink.write(
                {
                    "ID budovy": props.get("identifier"),
                    "Typ budovy": props.get("buildingtypecodename"),
                    "Typ budovy - kód": props.get("buildingtypecode"),
                    "Účel budovy": props.get("buildingpurposename"),
                    "Účel budovy - kód": props.get("buildingpurposecode"),
                    "Súpisné číslo": props.get("propertyregistrationnumber"),
                    "Časť obce": district_name,
                    "Časť obce - ID": district_id,
                    "Obec": region.obec_name,
                    "Obec - ID": region.obec_id,
                    "Okres": region.okres_name,
                    "Okres - ID": region.okres_id,
                    "Kraj": region.kraj_name,
                    "Kraj - ID": region.kraj_id,
                    "Platné od": validfrom,
                }
            )
            rows_written += 1

    return rows_written


def write_district_outputs(
    outputs: OutputManager,
    districts_by_id: dict[str, DistrictRecord],
) -> int:
    rows_written = 0

    for district_id in sorted(districts_by_id):
        entry = districts_by_id[district_id]
        outputs.districts.write(
            {
                "Časť obce": entry.district_name,
                "Časť obce - ID": entry.district_id,
                "Obec": entry.obec_name,
                "Obec - ID": entry.obec_id,
                "Okres": entry.okres_name,
                "Okres - ID": entry.okres_id,
                "Kraj": entry.kraj_name,
                "Kraj - ID": entry.kraj_id,
                "Platné od": entry.validfrom,
            }
        )
        rows_written += 1

    return rows_written


def write_audit_csv(path: Path, header: list[str], rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    sink = CsvSink(path, header)
    try:
        for row in rows:
            sink.write(row)
    finally:
        sink.close()


def write_link_audit_outputs(staging_dir: Path, audit: LinkAudit) -> list[Path]:
    audit_dir = staging_dir / "audit"
    written: list[Path] = []

    street_district_rows = sorted(
        audit.street_district_rows.values(),
        key=lambda row: (row.get("Kraj", ""), row.get("Okres", ""), row.get("Obec", ""), row.get("Ulica", ""), row.get("Ulica - ID", "")),
    )
    if street_district_rows:
        path = audit_dir / "street_district_conflicts.csv"
        write_audit_csv(path, STREET_DISTRICT_AUDIT_HEADER, street_district_rows)
        written.append(path)

    street_context_rows = sorted(
        audit.street_context_rows.values(),
        key=lambda row: (row.get("Kraj", ""), row.get("Okres", ""), row.get("Obec", ""), row.get("Ulica", ""), row.get("Ulica - ID", "")),
    )
    if street_context_rows:
        path = audit_dir / "street_context_conflicts.csv"
        write_audit_csv(path, STREET_CONTEXT_AUDIT_HEADER, street_context_rows)
        written.append(path)

    district_rows = sorted(
        audit.district_rows.values(),
        key=lambda row: (row.get("Kraj", ""), row.get("Okres", ""), row.get("Obec", ""), row.get("Časť obce", ""), row.get("Časť obce - ID", "")),
    )
    if district_rows:
        path = audit_dir / "district_conflicts.csv"
        write_audit_csv(path, DISTRICT_AUDIT_HEADER, district_rows)
        written.append(path)

    return written




SortSpec = list[tuple[str, bool, bool]]  # (column, natural_sort, empty_last)


def sort_text(value: Any) -> str:
    text = clean_str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    return text.casefold()


def natural_sort_key(value: Any) -> tuple[tuple[int, Any], ...]:
    text = sort_text(value)
    parts: list[tuple[int, Any]] = []
    for part in re.split(r"(\d+)", text):
        if not part:
            continue
        if part.isdigit():
            parts.append((0, int(part)))
        else:
            parts.append((1, part))
    return tuple(parts)


def csv_sort_value(row: dict[str, str], column: str, *, natural: bool = False, empty_last: bool = False) -> tuple[int, Any]:
    value = clean_str(row.get(column)).strip()
    empty_rank = 1 if empty_last and not value else 0
    key = natural_sort_key(value) if natural else sort_text(value)
    return empty_rank, key


def sort_csv_file(path: Path, spec: SortSpec) -> None:
    if not path.exists():
        return

    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            return
        fieldnames = list(reader.fieldnames)
        rows = list(reader)

    if len(rows) <= 1:
        return

    rows.sort(
        key=lambda row: tuple(
            csv_sort_value(row, column, natural=natural, empty_last=empty_last)
            for column, natural, empty_last in spec
        )
    )

    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def sort_generated_csvs(staging_dir: Path) -> None:
    jobs: list[tuple[Path, SortSpec]] = [
        (staging_dir / "nuts3.csv", [("Kraj", False, False)]),
        (staging_dir / "lau1.csv", [("Okres", False, False), ("Kraj", False, False)]),
        (staging_dir / "lau2.csv", [("Obec", False, False), ("Kraj", False, False), ("Okres", False, False)]),
        (staging_dir / "districts.csv", [("Časť obce", False, True), ("Obec", False, False), ("Okres", False, False), ("Kraj", False, False)]),
        (staging_dir / "streets.csv", [("Ulica", False, False), ("Obec", False, False), ("Časť obce", False, True), ("Ulica - ID", True, False)]),
    ]

    for path in sorted((staging_dir / "addresses").glob("*.csv")):
        jobs.append((path, [("Ulica", False, True), ("Orientačné číslo", True, True), ("ID budovy", True, False)]))

    for path in sorted((staging_dir / "buildings").glob("*.csv")):
        jobs.append((path, [("Obec", False, False), ("Časť obce", False, True), ("Súpisné číslo", True, True), ("ID budovy", True, False)]))

    print(f"Sorting {len(jobs)} generated CSV files...", flush=True)
    for path, spec in tqdm(jobs, desc="Sorting CSV files", unit="file"):
        sort_csv_file(path, spec)


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir
    refresh_days = effective_refresh_days(args.refresh_days)

    out_dir.mkdir(parents=True, exist_ok=True)

    if not should_refresh(out_dir, refresh_days, include_xlsx=args.write_xlsx):
        print(
            f"Skipping location refresh: all expected files exist and are newer than {refresh_days} days"
        )
        return 0

    staging_dir = prepare_staging_dir(out_dir)

    try:
        debug_dir = staging_dir / "debug" if args.debug else None
        if debug_dir is not None:
            debug_dir.mkdir(parents=True, exist_ok=True)
            print(f"Debug mode: dumping downloaded JSON payloads to {debug_dir}")

        session = requests.Session()

        print("Obtaining dataset catalog...")
        metadata_uris = parse_dataset_catalog(
            URI_ROOT,
            session=session,
            debug_dir=debug_dir,
        )

        if not metadata_uris:
            if has_existing_outputs(out_dir):
                return keep_existing_outputs_message(
                    f"failed to obtain dataset catalog from {URI_ROOT}", out_dir
                )
            print(f"Location refresh failed: could not obtain dataset catalog from {URI_ROOT}")
            return 1

        print(f"Resolving {len(metadata_uris)} dataset metadata records...")
        uris = resolve_dataset_uris(
            metadata_uris,
            session=session,
            debug_dir=debug_dir,
        )

        if not uris:
            if has_existing_outputs(out_dir):
                return keep_existing_outputs_message(
                    "catalog was fetched but no GeoJSON dataset URLs were resolved",
                    out_dir,
                )
            print("Location refresh failed: catalog was fetched but no GeoJSON dataset URLs were resolved")
            return 1

        groups = group_dataset_uris(uris)
        print(
            "Dataset groups: "
            f"nuts3={len(groups.nuts3)}, "
            f"lau1={len(groups.lau1)}, "
            f"lau2={len(groups.lau2)}, "
            f"streets={len(groups.streets)}, "
            f"addresses={len(groups.addresses)}, "
            f"buildings={len(groups.buildings)}, "
            f"other={len(groups.other)}"
        )

        if groups.other:
            print(f"Warning: found {len(groups.other)} unclassified dataset URIs")
            for uri in groups.other[:10]:
                print(f"  - {uri}")
            if len(groups.other) > 10:
                print("  ...")

        outputs = OutputManager(staging_dir)
        region_by_nuts3: dict[str, RegionInfo] = {}
        region_by_lau1: dict[str, RegionInfo] = {}
        region_by_lau2: dict[str, RegionInfo] = {}
        street_links: dict[str, StreetLink] = {}
        districts_by_id: dict[str, DistrictRecord] = {}
        stats = LinkStats()
        audit = LinkAudit()

        seen_nuts3: set[str] = set()
        seen_lau1: set[str] = set()
        seen_lau2: set[str] = set()

        try:
            process_nuts3(session, groups.nuts3, outputs, region_by_nuts3, debug_dir=debug_dir)
            process_lau1(session, groups.lau1, outputs, region_by_lau1, region_by_nuts3, debug_dir=debug_dir)
            process_lau2(session, groups.lau2, outputs, region_by_lau1, region_by_lau2, debug_dir=debug_dir)

            address_rows = process_maa(
                session,
                groups.addresses,
                outputs,
                street_links,
                districts_by_id,
                stats,
                audit,
                region_by_nuts3,
                region_by_lau1,
                region_by_lau2,
                seen_nuts3,
                seen_lau1,
                seen_lau2,
                debug_dir=debug_dir,
            )

            print(
                "Derived reference maps: "
                f"kraje={len(region_by_nuts3)}, "
                f"okresy={len(region_by_lau1)}, "
                f"obce={len(region_by_lau2)}"
            )

            process_msa(
                session,
                groups.streets,
                outputs,
                region_by_lau1,
                street_links,
                debug_dir=debug_dir,
            )

            building_rows = process_mba(
                session,
                groups.buildings,
                outputs,
                region_by_lau2,
                districts_by_id,
                stats,
                audit,
                debug_dir=debug_dir,
            )

            district_rows = write_district_outputs(outputs, districts_by_id)
            audit_files = write_link_audit_outputs(staging_dir, audit)
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

        sort_generated_csvs(staging_dir)

        xlsx_files: list[Path] = []
        if args.write_xlsx:
            xlsx_files = write_xlsx_for_csv_tree(staging_dir)
            print(f"Created {len(xlsx_files)} XLSX files")

        install_outputs(staging_dir, out_dir)

        print(
            f"Wrote {address_rows} address rows, {building_rows} building rows, "
            f"{district_rows} districts, {len(region_by_nuts3)} kraje, "
            f"{len(region_by_lau1)} okresy, {len(region_by_lau2)} obce"
        )
        print(
            "Link audit: "
            f"street->district conflicts={stats.street_district_conflicts}, "
            f"street context conflicts={stats.street_context_conflicts}, "
            f"district conflicts={stats.district_conflicts}"
        )

        if audit_files:
            print("Audit files:")
            for path in audit_files:
                print(f"  - {out_dir / path.relative_to(staging_dir)}")

        return 0

    finally:
        cleanup_staging_dir(staging_dir)


if __name__ == "__main__":
    raise SystemExit(main())
