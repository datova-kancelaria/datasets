from __future__ import annotations

import os, json
from pathlib import Path

from helper import (
    fetch_paged, fetch_attr_metadata, get_enums_needed, sanitize_node,
    load_merge_schema, merge_attribute_metadata, remap_entities,
    write_excel, standardize_data,
)

def main() -> int:
    api_uri = os.getenv("API_URI", "")
    api_report_num = os.getenv("METAIS_REPORT_NUM_PROD", "")
    egov_dir = Path("data/egov")
    raw_dump_dir = egov_dir / "raw"
    raw_dump_dir.mkdir(parents=True, exist_ok=True)

    if not api_uri and not api_report_num:
        print('One of the env variables "API_URI"/"METAIS_REPORT_NUM_PROD" must be set!', file=sys.stderr)
        return 2
    elif api_report_num:
        api_uri = "https://metais.slovensko.sk/api/report/reports/execute/" + api_report_num + "/type/typ?lang=sk"

    schema_path = Path(__file__).with_name("sync_params_AS_IS.json")
    if not schema_path.exists():
        print(f"Schema file not found: {schema_path}", file=sys.stderr)
        return 3

    schema = load_merge_schema(schema_path)

    print("Fetching AS...", flush=True)
    AS_data = fetch_paged(api_uri, "AS", page_size=5000, sleep_ms=250)

    print("Fetching InfraSluzba...", flush=True)
    IS_data = fetch_paged(api_uri, "InfraSluzba", page_size=5000, sleep_ms=250)

    print("Fetching attribute metadata...", flush=True)
    AS_metadata = fetch_attr_metadata("AS")
    IS_metadata = fetch_attr_metadata("InfraSluzba")

    enums: Dict[str, Dict[str, str]] = {}
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

    # merged metadata from schema (only valid params)
    merged_meta = merge_attribute_metadata(AS_metadata, IS_metadata, schema)
    (raw_dump_dir / "AS_IS_merged_meta.json").write_text(
        json.dumps(merged_meta, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    AS_harmonized = remap_entities(AS_clean, entity_kind="AS", schema=schema, merged_meta=merged_meta)
    IS_harmonized = remap_entities(IS_clean, entity_kind="InfraSluzba", schema=schema, merged_meta=merged_meta)

    (raw_dump_dir / "AS_harmonized.json").write_text(json.dumps(AS_harmonized, ensure_ascii=False, indent=2), encoding="utf-8")
    (raw_dump_dir / "InfraSluzba_harmonized.json").write_text(json.dumps(IS_harmonized, ensure_ascii=False, indent=2), encoding="utf-8")

    # single combined dataset
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
