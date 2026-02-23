import json, csv, os, sys

data_dir = "data/egov"

def standardize_data(data):
    res = data["result"]
    header = [h["name"] for h in res.get("headers", [])]
    rows = [r.get("values", []) for r in res.get("rows", [])]
    return {"header": header, "rows": rows}

for filename in os.listdir(data_dir):
    if not filename.endswith(".json"):
        continue
    infile = os.path.join(data_dir, filename)
    outfile = os.path.join(data_dir, filename.replace(".json", ".csv"))

    with open(infile, "r", encoding="utf-8") as f:
        data = json.load(f)

    headers = [h["name"] for h in data["result"]["headers"]]
    rows = [r["values"] for r in data["result"]["rows"]]

    # Write CSV (semicolon delimiter, UTF-8 BOM for Excel)
    with open(outfile, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.writer(f, delimiter=";", quoting=csv.QUOTE_ALL)
        writer.writerow(headers)
        for r in rows:
            # Replace None with empty string
            writer.writerow(["" if v is None else v for v in r])

    # overwrite json with leaner format
    data_standardized = standardize_data(data)
    with open(infile, "w", encoding="utf-8") as f:
        f.write(json.dumps(data_standardized, ensure_ascii=False, indent=2))
