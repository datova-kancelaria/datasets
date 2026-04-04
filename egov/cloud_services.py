from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from helper import (
    fetch_cilistfiltered_windowed,
    fetch_attr_metadata,
    get_enums_needed,
    sanitize_node,
    load_merge_schema,
    merge_attribute_metadata,
    remap_entities,
    write_excel,
    standardize_data,
)


def _default_created_at_to() -> str:
    return datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "egov",
        help="Directory where egov cloud-services outputs will be written",
    )
    parser.add_argument(
        "--created-at-from",
        default="2000-01-01T00:00:00.000",
        help="Lower bound for CMDB createdAt filtering (inclusive)",
    )
    parser.add_argument(
        "--created-at-to",
        default=_default_created_at_to(),
        help="Upper bound for CMDB createdAt filtering (inclusive)",
    )
    parser.add_argument(
        "--window-target-count",
        type=int,
        default=9500,
        help="Split createdAt windows until probe count is strictly below this many records",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=1000,
        help="Page size for cilistfiltered page fetches",
    )
    parser.add_argument(
        "--probe-page-size",
        type=int,
        default=1,
        help="Probe page size used while estimating window sizes",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    egov_dir = args.out_dir
    raw_dump_dir = egov_dir / "raw"
    raw_dump_dir.mkdir(parents=True, exist_ok=True)

    schema_path = Path(__file__).with_name("sync_params_AS_IS.json")
    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}")
        return 3

    schema = load_merge_schema(schema_path)

    print(
        f"Fetching AS via public cilistfiltered createdAt=[{args.created_at_from}, {args.created_at_to}]...",
        flush=True,
    )
    AS_data, AS_windows = fetch_cilistfiltered_windowed(
        "AS",
        created_at_from=args.created_at_from,
        created_at_to=args.created_at_to,
        window_target_count=args.window_target_count,
        page_size=args.page_size,
        probe_page_size=args.probe_page_size,
    )

    print(
        f"Fetching InfraSluzba via public cilistfiltered createdAt=[{args.created_at_from}, {args.created_at_to}]...",
        flush=True,
    )
    IS_data, IS_windows = fetch_cilistfiltered_windowed(
        "InfraSluzba",
        created_at_from=args.created_at_from,
        created_at_to=args.created_at_to,
        window_target_count=args.window_target_count,
        page_size=args.page_size,
        probe_page_size=args.probe_page_size,
    )

    print("Fetching attribute metadata...", flush=True)
    AS_metadata = fetch_attr_metadata("AS")
    IS_metadata = fetch_attr_metadata("InfraSluzba")

    enums: dict[str, dict[str, str]] = {}
    print("Fetching enums (AS)...", flush=True)
    get_enums_needed(AS_metadata, enums)
    print("Fetching enums (InfraSluzba)...", flush=True)
    get_enums_needed(IS_metadata, enums)

    print("Sanitizing nodes...", flush=True)
    AS_clean = sanitize_node(AS_data, AS_metadata, enums)
    IS_clean = sanitize_node(IS_data, IS_metadata, enums)

    raw_dump_AS = raw_dump_dir / "AS.json"
    raw_dump_IS = raw_dump_dir / "InfraSluzba.json"
    raw_dump_AS_meta = raw_dump_dir / "AS_meta.json"
    raw_dump_IS_meta = raw_dump_dir / "InfraSluzba_meta.json"

    raw_dump_AS.write_text(json.dumps(AS_clean, ensure_ascii=False, indent=2), encoding="utf-8")
    raw_dump_IS.write_text(json.dumps(IS_clean, ensure_ascii=False, indent=2), encoding="utf-8")
    raw_dump_AS_meta.write_text(json.dumps(AS_metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    raw_dump_IS_meta.write_text(json.dumps(IS_metadata, ensure_ascii=False, indent=2), encoding="utf-8")
    (raw_dump_dir / "AS_fetch_windows.json").write_text(json.dumps(AS_windows, ensure_ascii=False, indent=2), encoding="utf-8")
    (raw_dump_dir / "InfraSluzba_fetch_windows.json").write_text(json.dumps(IS_windows, ensure_ascii=False, indent=2), encoding="utf-8")

    merged_meta = merge_attribute_metadata(AS_metadata, IS_metadata, schema)
    (raw_dump_dir / "AS_IS_merged_meta.json").write_text(
        json.dumps(merged_meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    AS_harmonized = remap_entities(AS_clean, entity_kind="AS", schema=schema, merged_meta=merged_meta)
    IS_harmonized = remap_entities(IS_clean, entity_kind="InfraSluzba", schema=schema, merged_meta=merged_meta)

    (raw_dump_dir / "AS_harmonized.json").write_text(json.dumps(AS_harmonized, ensure_ascii=False, indent=2), encoding="utf-8")
    (raw_dump_dir / "InfraSluzba_harmonized.json").write_text(json.dumps(IS_harmonized, ensure_ascii=False, indent=2), encoding="utf-8")

    combined = AS_harmonized + IS_harmonized
    (raw_dump_dir / "AS_IS_combined.json").write_text(json.dumps(combined, ensure_ascii=False, indent=2), encoding="utf-8")

    print(f"Done. AS={len(AS_clean)} InfraSluzba={len(IS_clean)} merged_meta={len(merged_meta)}", flush=True)

    important = [
        "Gen_Profil_nazov",
        "Gen_Profil_popis",
        "Gen_Profil_anglicky_nazov",
        "EA_Profil_typ_cloudovej_sluzby",
        "Gen_Profil_kod_metais",
        "Gen_Profil_ref_id",
    ]

    xlsx_path = egov_dir / "CloudSluzba.xlsx"
    write_excel(
        xlsx_path,
        combined,
        merged_meta,
        attr_order=important,
        sort_by="Gen_Profil_nazov",
        header_incl_tech_name=True,
    )
    print(f"Wrote Excel: {xlsx_path}", flush=True)

    xlsx_path10 = egov_dir / "CloudSluzba_curated.xlsx"
    write_excel(
        xlsx_path10,
        combined,
        merged_meta,
        attr_order=important,
        sort_by="Gen_Profil_nazov",
        drop_param_threshold=0.1,
    )
    print(f"Wrote Excel: {xlsx_path10}", flush=True)

    path_json_res = egov_dir / "CloudSluzba.json"
    standard_format_json = standardize_data(combined, merged_meta, attr_order=important, sort_by="Gen_Profil_nazov")
    path_json_res.write_text(json.dumps(standard_format_json, ensure_ascii=False, indent=2), encoding="utf-8")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
