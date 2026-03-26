from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import Any

import pandas as pd
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
            return [o["iri"] for o in ds]
    return []


def collect_features(uris: list[str]) -> list[dict[str, Any]]:
    s = requests.Session()
    features: list[dict[str, Any]] = []

    for uri in tqdm(uris, desc="Fetching datasets", unit="dataset"):
        r = fetch_json_with_retry(uri, session=s, tries=6, timeout=30.0, backoff=1.7)
        if r is None:
            continue

        current = r.get("features", [])
        if current:
            for feature in current:
                if isinstance(feature, dict):
                    features.append(feature)
        else:
            print(f"Warning: features in {uri} is empty/does not exist!")

    return features


def create_table(features: list[dict[str, Any]]) -> dict[str, list[str]]:
    ids: list[str] = []
    nuts3_names: list[str] = []
    nuts3_ids: list[str] = []
    lau1_names: list[str] = []
    lau1_ids: list[str] = []
    lau2_names: list[str] = []
    lau2_ids: list[str] = []
    district_names: list[str] = []
    streetnames: list[str] = []
    propertyregistrationnumbers: list[str] = []
    orientationnumbers: list[str] = []
    postalcodes: list[str] = []
    coord_x: list[str] = []
    coord_y: list[str] = []

    for feature in features:
        properties = feature.get("properties") or {}
        geometry = feature.get("geometry") or {}
        coords = geometry.get("coordinates") or []
        x = coords[0] if len(coords) >= 2 else ""
        y = coords[1] if len(coords) >= 2 else ""

        ids.append(properties.get("identifier", ""))
        nuts3_names.append(properties.get("nuts3_name", ""))
        nuts3_ids.append(properties.get("nuts3_id", ""))
        lau1_names.append(properties.get("lau1_name", ""))
        lau1_ids.append(properties.get("lau1_id", ""))
        lau2_names.append(properties.get("lau2_name", ""))
        lau2_ids.append(properties.get("lau2_id", ""))
        district_names.append(properties.get("district_name", ""))
        streetnames.append(properties.get("streetname", ""))
        propertyregistrationnumbers.append(properties.get("propertyregistrationnumber", ""))
        orientationnumbers.append(properties.get("orientationnumber", ""))
        postalcodes.append(properties.get("postalcode", ""))
        coord_x.append(x)
        coord_y.append(y)

    return {
        "Identifikátor": ids,
        "Kraj": nuts3_names,
        "ID kraja": nuts3_ids,
        "Okres": lau1_names,
        "ID Okresu": lau1_ids,
        "Obec": lau2_names,
        "ID obce": lau2_ids,
        "Časť obce": district_names,
        "Ulica": streetnames,
        "Súpisné číslo": propertyregistrationnumbers,
        "Orientačné číslo celé": orientationnumbers,
        "PSČ": postalcodes,
        "ADRBOD_X": coord_x,
        "ADRBOD_Y": coord_y,
    }


def main() -> int:
    args = parse_args()
    out_dir = args.out_dir
    refresh_days = effective_refresh_days(args.refresh_days)
    sentinel = out_dir / "kraje.csv"

    out_dir.mkdir(parents=True, exist_ok=True)

    if not should_refresh(out_dir, refresh_days):
        print(f"Skipping NUTS refresh: all expected files exist and are newer than {refresh_days} days")
        return 0

    print("Obtaining URIs...")
    uris = parse_dataset(URI_root)

    print("Fetching features...")
    features = collect_features(uris)

    print("Creating table...")
    table = create_table(features)
    df = pd.DataFrame(table)

    print("Sorting by Kraj...")
    df = df.sort_values("Kraj", kind="stable")

    print("Saving to kraje...")
    df.to_csv(out_dir / "kraje.csv", index=False)

    known = set(name_to_abb.keys())
    seen = {k for k in df["Kraj"].dropna().unique() if str(k).strip() != ""}

    missing = sorted(seen - known)
    if missing:
        print("Warning: Kraj values not in name_to_abb:")
        for kraj in missing:
            print(f"  - {kraj}")

    print("Saving to individual...")
    for kraj, abb in name_to_abb.items():
        df_k = df[df["Kraj"] == kraj]
        if df_k.empty:
            continue
        df_k.to_csv(out_dir / f"{abb}.csv", index=False)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
