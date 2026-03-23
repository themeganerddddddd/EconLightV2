from pathlib import Path
import json
import math
import pandas as pd

DERIVED = Path("data/derived")
DOCS_DATA = Path("docs/data")
DOCS_DATA.mkdir(parents=True, exist_ok=True)


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
leaders = pd.read_csv(DERIVED / "leaders.csv")
laggards = pd.read_csv(DERIVED / "laggards.csv")
index_df = pd.read_csv(DERIVED / "headline_index.csv")
hist = pd.read_csv(DERIVED / "region_month_metrics.csv")

for df in [latest, leaders, laggards, index_df, hist]:
    if "date" in df.columns:
        df["date"] = df["date"].astype(str)

latest_records = clean_records(latest.to_dict(orient="records"))
leaders_records = clean_records(leaders.to_dict(orient="records"))
laggards_records = clean_records(laggards.to_dict(orient="records"))
index_records = clean_records(index_df.to_dict(orient="records"))

histories = {}
for region_id, sub in hist.groupby("region_id"):
    records = sub.sort_values("date").to_dict(orient="records")
    histories[str(region_id)] = clean_records(records)

summary = {}
if len(index_df) > 0 and len(latest) > 0:
    latest_index = index_df.iloc[-1]
    top_region = latest.sort_values("trend_score", ascending=False).iloc[0]
    bottom_region = latest.sort_values("trend_score", ascending=True).iloc[0]

    summary = {
        "latest_month": clean_value(latest_index["date"]),
        "headline_index": clean_value(latest_index["index_level"]),
        "avg_mom": clean_value(latest_index.get("avg_mom")),
        "avg_yoy": clean_value(latest_index.get("avg_yoy")),
        "top_region": {
            "region_id": clean_value(top_region["region_id"]),
            "region_name": clean_value(top_region["region_name"]),
            "trend_score": clean_value(top_region["trend_score"]),
            "yoy_pct": clean_value(top_region["yoy_pct"]),
        },
        "bottom_region": {
            "region_id": clean_value(bottom_region["region_id"]),
            "region_name": clean_value(bottom_region["region_name"]),
            "trend_score": clean_value(bottom_region["trend_score"]),
            "yoy_pct": clean_value(bottom_region["yoy_pct"]),
        },
    }

with open(DOCS_DATA / "latest.json", "w", encoding="utf-8") as f:
    json.dump(latest_records, f, indent=2, allow_nan=False)

with open(DOCS_DATA / "leaders.json", "w", encoding="utf-8") as f:
    json.dump(leaders_records, f, indent=2, allow_nan=False)

with open(DOCS_DATA / "laggards.json", "w", encoding="utf-8") as f:
    json.dump(laggards_records, f, indent=2, allow_nan=False)

with open(DOCS_DATA / "index.json", "w", encoding="utf-8") as f:
    json.dump(index_records, f, indent=2, allow_nan=False)

with open(DOCS_DATA / "histories.json", "w", encoding="utf-8") as f:
    json.dump(histories, f, indent=2, allow_nan=False)

with open(DOCS_DATA / "summary.json", "w", encoding="utf-8") as f:
    json.dump(summary, f, indent=2, allow_nan=False)

print("Exported clean site data.")