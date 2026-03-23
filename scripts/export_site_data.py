from pathlib import Path
import json
import math
import pandas as pd

DERIVED = Path("data/derived")
SITE_DATA = Path("site/data")
SITE_DATA.mkdir(parents=True, exist_ok=True)


def clean_value(x):
    if pd.isna(x):
        return None
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    return x


def clean_records(records):
    cleaned = []
    for row in records:
        cleaned.append({k: clean_value(v) for k, v in row.items()})
    return cleaned


latest = pd.read_csv(DERIVED / "latest_rankings.csv")
index_df = pd.read_csv(DERIVED / "headline_index.csv")
hist = pd.read_csv(DERIVED / "region_month_metrics.csv")

for df in [latest, index_df, hist]:
    if "date" in df.columns:
        df["date"] = df["date"].astype(str)

latest_records = clean_records(latest.to_dict(orient="records"))
index_records = clean_records(index_df.to_dict(orient="records"))

histories = {}
for region_id, sub in hist.groupby("region_id"):
    records = sub.sort_values("date").to_dict(orient="records")
    histories[str(region_id)] = clean_records(records)

with open(SITE_DATA / "latest.json", "w", encoding="utf-8") as f:
    json.dump(latest_records, f, indent=2, allow_nan=False)

with open(SITE_DATA / "index.json", "w", encoding="utf-8") as f:
    json.dump(index_records, f, indent=2, allow_nan=False)

with open(SITE_DATA / "histories.json", "w", encoding="utf-8") as f:
    json.dump(histories, f, indent=2, allow_nan=False)

print("Exported clean site data.")