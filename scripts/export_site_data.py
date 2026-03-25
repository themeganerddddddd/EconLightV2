from pathlib import Path
import json
import math

import numpy as np
import pandas as pd

DERIVED = Path("data/derived")
DOCS_DATA = Path("docs/data")
COUNTY_SHARDS_DIR = DOCS_DATA / "counties_histories_shards"

DOCS_DATA.mkdir(parents=True, exist_ok=True)
COUNTY_SHARDS_DIR.mkdir(parents=True, exist_ok=True)


def clean_value(x):
    if pd.isna(x):
        return None

    if isinstance(x, np.integer):
        return int(x)

    if isinstance(x, np.floating):
        xf = float(x)
        if math.isnan(xf) or math.isinf(xf):
            return None
        return xf

    if isinstance(x, float):
        if math.isnan(x) or math.isinf(x):
            return None
        return x

    if isinstance(x, pd.Timestamp):
        return str(x)

    return x


def clean_records(records):
    return [{k: clean_value(v) for k, v in row.items()} for row in records]


def write_json(path: Path, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)


def export_standard_dataset(dataset_name: str):
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

    write_json(DOCS_DATA / f"{dataset_name}_latest.json", latest_records)
    write_json(DOCS_DATA / f"{dataset_name}_leaders.json", leaders_records)
    write_json(DOCS_DATA / f"{dataset_name}_laggards.json", laggards_records)
    write_json(DOCS_DATA / f"{dataset_name}_index.json", index_records)
    write_json(DOCS_DATA / f"{dataset_name}_histories.json", histories)
    write_json(DOCS_DATA / f"{dataset_name}_summary.json", summary)
    write_json(DOCS_DATA / f"{dataset_name}_regions.json", regions)

    print(f"{dataset_name}: exported site data")


def export_counties_dataset():
    dataset_name = "counties"

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

    for df in [latest, leaders, laggards, hist]:
        if "region_id" in df.columns:
            df["region_id"] = df["region_id"].astype(str).str.zfill(5)
        if "region_name" in df.columns:
            df["region_name"] = df["region_name"].astype(str)

    for df in [latest, leaders, laggards, index_df, hist]:
        if "date" in df.columns:
            df["date"] = df["date"].astype(str)

    latest_records = clean_records(latest.to_dict(orient="records"))
    leaders_records = clean_records(leaders.to_dict(orient="records"))
    laggards_records = clean_records(laggards.to_dict(orient="records"))
    index_records = clean_records(index_df.to_dict(orient="records"))

    regions = []
    shard_index = {}

    # Clear old shard files
    for old_file in COUNTY_SHARDS_DIR.glob("*.json"):
        old_file.unlink()

    for region_id, sub in hist.groupby("region_id"):
        region_id = str(region_id).zfill(5)
        sub = sub.sort_values("date")
        statefp = region_id[:2]

        shard_index[region_id] = statefp
        regions.append({
            "region_id": region_id,
            "region_name": str(sub.iloc[-1]["region_name"]),
            "statefp": statefp,
        })

    regions = sorted(regions, key=lambda x: x["region_name"])

    # Write one file per state shard
    for statefp, sub in hist.groupby(hist["region_id"].astype(str).str.zfill(5).str[:2]):
        shard_payload = {}
        sub = sub.copy()
        sub["region_id"] = sub["region_id"].astype(str).str.zfill(5)

        for region_id, region_sub in sub.groupby("region_id"):
            region_sub = region_sub.sort_values("date")
            shard_payload[str(region_id)] = clean_records(region_sub.to_dict(orient="records"))

        write_json(COUNTY_SHARDS_DIR / f"{statefp}.json", shard_payload)

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
                "region_id": clean_value(str(top_region["region_id"]).zfill(5)),
                "region_name": clean_value(top_region["region_name"]),
                "trend_score": clean_value(top_region["trend_score"]),
                "yoy_pct": clean_value(top_region["yoy_pct"]),
            },
            "bottom_region": {
                "region_id": clean_value(str(bottom_region["region_id"]).zfill(5)),
                "region_name": clean_value(bottom_region["region_name"]),
                "trend_score": clean_value(bottom_region["trend_score"]),
                "yoy_pct": clean_value(bottom_region["yoy_pct"]),
            },
        }

    write_json(DOCS_DATA / "counties_latest.json", latest_records)
    write_json(DOCS_DATA / "counties_leaders.json", leaders_records)
    write_json(DOCS_DATA / "counties_laggards.json", laggards_records)
    write_json(DOCS_DATA / "counties_index.json", index_records)
    write_json(DOCS_DATA / "counties_summary.json", summary)
    write_json(DOCS_DATA / "counties_regions.json", regions)
    write_json(DOCS_DATA / "counties_histories_index.json", shard_index)

    print("counties: exported site data with sharded histories")


def main():
    for dataset in ["metros", "states", "cities"]:
        export_standard_dataset(dataset)
    export_counties_dataset()


if __name__ == "__main__":
    main()