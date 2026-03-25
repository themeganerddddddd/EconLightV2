from pathlib import Path
import json
import math

import numpy as np
import pandas as pd

DERIVED = Path("data/derived")
DOCS_DATA = Path("docs/data")
DOCS_DATA.mkdir(parents=True, exist_ok=True)


def clean_value(x):
    # Missing values
    if pd.isna(x):
        return None

    # NumPy scalar types -> native Python types
    if isinstance(x, np.integer):
        return int(x)

    if isinstance(x, np.floating):
        xf = float(x)
        if math.isnan(xf) or math.isinf(xf):
            return None
        return xf

    # Plain Python floats
    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        return x

    # Timestamps -> string
    if isinstance(x, (pd.Timestamp,)):
        return str(x)

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
        print(f"{dataset_name}: missing derived files, skipping")
        return

    latest = pd.read_csv(latest_path, low_memory=False)
    leaders = pd.read_csv(leaders_path, low_memory=False)
    laggards = pd.read_csv(laggards_path, low_memory=False)
    index_df = pd.read_csv(index_path, low_memory=False)
    hist = pd.read_csv(hist_path, low_memory=False)

    # Keep IDs/names as strings where appropriate
    for df in [latest, leaders, laggards, hist]:
        if "region_id" in df.columns:
            df["region_id"] = df["region_id"].astype(str)
        if "region_name" in df.columns:
            df["region_name"] = df["region_name"].astype(str)

    for df in [latest, leaders, laggards, index_df, hist]:
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)

    latest_records = clean_records(latest.to_dict(orient="records"))
    leaders_records = clean_records(leaders.to_dict(orient="records"))
    laggards_records = clean_records(laggards.to_dict(orient="records"))
    index_records = clean_records(index_df.to_dict(orient="records"))

    histories = {}
    regions = []

    for region_id, sub in hist.groupby("region_id"):
        sub = sub.sort_values("date")
        histories[str(region_id)] = clean_records(sub.to_dict(orient="records"))
        regions.append({
            "region_id": str(region_id),
            "region_name": str(sub.iloc[-1]["region_name"])
        })

    regions = sorted(regions, key=lambda x: x["region_name"])

    summary = {}
    if len(index_df) > 0 and len(latest) > 0:
        latest_index = index_df.iloc[-1]
        top_region = latest.sort_values("trend_score", ascending=False).iloc[0]
        bottom_region = latest.sort_values("trend_score", ascending=True).iloc[0]

        summary = clean_value({
            "dataset_name": dataset_name,
            "latest_month": latest_index["date"],
            "headline_index": latest_index.get("index_level"),
            "avg_mom": latest_index.get("avg_mom"),
            "avg_yoy": latest_index.get("avg_yoy"),
            "top_region": {
                "region_id": top_region["region_id"],
                "region_name": top_region["region_name"],
                "trend_score": top_region.get("trend_score"),
                "yoy_pct": top_region.get("yoy_pct"),
            },
            "bottom_region": {
                "region_id": bottom_region["region_id"],
                "region_name": bottom_region["region_name"],
                "trend_score": bottom_region.get("trend_score"),
                "yoy_pct": bottom_region.get("yoy_pct"),
            },
        })

        # clean nested dict
        summary = json.loads(json.dumps(summary, default=clean_value))

    outputs = {
        f"{dataset_name}_latest.json": latest_records,
        f"{dataset_name}_leaders.json": leaders_records,
        f"{dataset_name}_laggards.json": laggards_records,
        f"{dataset_name}_index.json": index_records,
        f"{dataset_name}_histories.json": histories,
        f"{dataset_name}_summary.json": summary,
        f"{dataset_name}_regions.json": regions,
    }

    for filename, payload in outputs.items():
        with open(DOCS_DATA / filename, "w", encoding="utf-8") as f:
            json.dump(payload, f, indent=2, allow_nan=False)

    print(f"{dataset_name}: exported site data")


def main():
    for dataset in ["metros", "states", "counties", "cities"]:
        export_dataset(dataset)


if __name__ == "__main__":
    main()