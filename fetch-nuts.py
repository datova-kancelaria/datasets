from __future__ import annotations

import time
import json
import requests
import pandas as pd
from typing import TypeAlias, Any
from tqdm import tqdm
from pathlib import Path

URI_root = "https://rageo.minv.sk/opendata/katalog.json"

def fetch_json_with_retry(
    url: str,
    *,
    session: requests.Session | None = None,
    tries: int = 5,
    timeout: float = 30.0,
    backoff: float = 1.5,
) -> dict[str, Any] | None:
    s = session or requests
    last_err: Exception | None = None

    for attempt in range(tries):
        try:
            resp = s.get(url, timeout=timeout)
            resp.raise_for_status()
            return resp.json()
        except (requests.exceptions.RequestException, ValueError) as e:
            last_err = e
            if attempt < tries - 1:
                time.sleep(backoff ** attempt)
            else:
                print(f"[FAIL] {url} after {tries} tries: {e}")

    return None

def parse_dataset(uri: str) -> list[str]:
    r = fetch_json_with_retry(uri)

    graph = r.get("@graph", [])

    for g in graph:
        ds = g.get("dcat:dataset", None)
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

        F = r.get("features", [])
        if F:
            for f in F:
                if isinstance(f, dict):
                    features.append(f)
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
    res = {
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
        "ADRBOD_Y": coord_y
    }

    return res

name_to_abb = {
    "Banskobystrický": "BBSK",
    "Bratislavský": "BSK",
    "Nitriansky": "NSK",
    "Košický": "KSK",
    "Prešovský": "PSK",
    "Trenčiansky": "TSK",
    "Trnavský": "TTSK",
    "Žilinský": "ZSK"
}

dest_root = "data/egov/"
Path(dest_root).mkdir(parents=True, exist_ok=True)

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

df.to_csv(dest_root + "kraje.csv", index=False)

'''
MAX_ROWS = 1_048_576 # magic number 
chunk_rows = MAX_ROWS - 1  # keep header

n = len(df)
parts = (n + chunk_rows - 1) // chunk_rows

for i in range(parts):
    lo = i * chunk_rows
    hi = min(n, (i + 1) * chunk_rows)
    out = dest_root + f"kraje_part{i+1:02d}.xlsx"
    print(f"Saving {out} rows {lo}:{hi} ...")
    df.iloc[lo:hi].to_excel(out, index=False)
'''

known = set(name_to_abb.keys())
seen = {k for k in df["Kraj"].dropna().unique() if str(k).strip() != ""}

missing = sorted(seen - known)
if missing:
    print("Warning: Kraj values not in name_to_abb:")
    for k in missing:
        print(f"  - {k}")

print("Saving to individual...")

for kraj, abb in name_to_abb.items():
    df_k = df[df["Kraj"] == kraj]
    if df_k.empty:
        continue

    df_k.to_csv(dest_root + f"{abb}.csv", index=False)

    '''
    if len(df_k) <= MAX_ROWS:
        df_k.to_excel(dest_root + f"{abb}.xlsx", index=False)
    else:
        print(f"Skipping Excel for {kraj} ({abb}): {len(df_k)} rows > Excel limit")
    '''