from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path


EXPECTED_REPORTS = {
    "KS.json",
    "AS.json",
    "ISVS.json",
    "Projekt.json",
    "InfraSluzba.json",
    "KRIS.json",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=Path(__file__).resolve().parents[1] / "data" / "egov",
        help="Directory containing egov JSON reports",
    )
    return parser.parse_args()


def standardize_data(data: dict) -> dict:
    res = data["result"]
    header = [h["name"] for h in res.get("headers", [])]
    rows = [r.get("values", []) for r in res.get("rows", [])]
    return {"header": header, "rows": rows}


def convert_one(infile: Path) -> None:
    outfile = infile.with_suffix(".csv")

    with infile.open("r", encoding="utf-8") as f:
        data = json.load(f)

    if "result" not in data:
        raise ValueError(f"{infile} does not have top-level 'result'")

    headers = [h["name"] for h in data["result"]["headers"]]
    rows = [r["values"] for r in data["result"]["rows"]]

    with outfile.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        for row in rows:
            writer.writerow(["" if value is None else value for value in row])

    with infile.open("w", encoding="utf-8") as f:
        json.dump(standardize_data(data), f, ensure_ascii=False, indent=2)


def main() -> int:
    args = parse_args()
    data_dir = args.data_dir
    data_dir.mkdir(parents=True, exist_ok=True)

    for infile in sorted(data_dir.iterdir()):
        if infile.name not in EXPECTED_REPORTS:
            continue
        convert_one(infile)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
