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
    return [{k: clean_value(v) for k, v in row.items()} for row in records]


def export_dataset(dataset_name: str):
    latest_path = DERIVED / f"{dataset_name}_latest_rankings.csv"
    leaders_path = DERIVED / f"{dataset_name}_leaders.csv"
    laggards_path = DERIVED / f"{dataset_name}_laggards.csv"
    index_path = DERIVED / f"{dataset_name}_headline_index.csv"
    hist_path = DERIVED / f"{dataset_name}_region_month_metrics.csv"

    if not all(p.exists() for p in [latest_path, leaders_path, laggards_path, index_path, hist_path]):
        print(f"{dataset_name}: missing one or more derived files, skipping export")
        return

    latest = pd.read_csv(latest_path)
    leaders = pd.read_csv(leaders_path)
    laggards = pd.read_csv(laggards_path)
    index_df = pd.read_csv(index_path)
    hist = pd.read_csv(hist_path)

    for df in [latest, leaders, laggards, index_df, hist]:
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)

    latest_records = clean_records(latest.to_dict(orient="records"))
    leaders_records = clean_records(leaders.to_dict(orient="records"))
    laggards_records = clean_records(laggards.to_dict(orient="records"))
    index_records = clean_records(index_df.to_dict(orient="records"))

    histories = {}
    region_list = []

    for region_id, sub in hist.groupby("region_id"):
        sub = sub.sort_values("date")
        histories[str(region_id)] = clean_records(sub.to_dict(orient="records"))
        region_list.append(
            {
                "region_id": str(region_id),
                "region_name": str(sub.iloc[-1]["region_name"]),
            }
        )

    region_list = sorted(region_list, key=lambda x: x["region_name"])

    summary = {}
    if len(index_df) > 0 and len(latest) > 0:
        latest_index = index_df.iloc[-1]
        top_region = latest.sort_values("trend_score", ascending=False).iloc[0]
        bottom_region = latest.sort_values("trend_score", ascending=True).iloc[0]

        summary = {
            "dataset_name": dataset_name,
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

    with open(DOCS_DATA / f"{dataset_name}_latest.json", "w", encoding="utf-8") as f:
        json.dump(latest_records, f, indent=2, allow_nan=False)

    with open(DOCS_DATA / f"{dataset_name}_leaders.json", "w", encoding="utf-8") as f:
        json.dump(leaders_records, f, indent=2, allow_nan=False)

    with open(DOCS_DATA / f"{dataset_name}_laggards.json", "w", encoding="utf-8") as f:
        json.dump(laggards_records, f, indent=2, allow_nan=False)

    with open(DOCS_DATA / f"{dataset_name}_index.json", "w", encoding="utf-8") as f:
        json.dump(index_records, f, indent=2, allow_nan=False)

    with open(DOCS_DATA / f"{dataset_name}_histories.json", "w", encoding="utf-8") as f:
        json.dump(histories, f, indent=2, allow_nan=False)

    with open(DOCS_DATA / f"{dataset_name}_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, allow_nan=False)

    with open(DOCS_DATA / f"{dataset_name}_regions.json", "w", encoding="utf-8") as f:
        json.dump(region_list, f, indent=2, allow_nan=False)

    print(f"{dataset_name}: exported site data")


def main():
    export_dataset("metros")
    export_dataset("states")


if __name__ == "__main__":
    main()