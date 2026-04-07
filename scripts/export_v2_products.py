from pathlib import Path
import json
import math
import numpy as np
import pandas as pd

DOCS = Path("docs/data")
DOCS.mkdir(parents=True, exist_ok=True)
DERIVED = Path("data/derived")
V2 = Path("data/v2")

DATASETS = ["states", "metros", "counties", "cities"]

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
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, allow_nan=False)

def add_rank_percentile(df, value_col="trend_score", ascending=False):
    out = df.copy()
    out = out.sort_values(value_col, ascending=ascending).reset_index(drop=True)
    out["rank_overall"] = np.arange(1, len(out) + 1)
    if len(out) > 1:
        out["percentile"] = 1 - ((out["rank_overall"] - 1) / (len(out) - 1))
    else:
        out["percentile"] = 1.0
    out["percentile"] = (out["percentile"] * 100).round(1)
    out["direction"] = np.where(
        out["yoy_pct_display"].fillna(0) > 0.005,
        "Up",
        np.where(out["yoy_pct_display"].fillna(0) < -0.005, "Down", "Flat")
    )
    return out

def build_profile_index():
    rows = []

    for dataset in DATASETS:
        latest_path = DERIVED / f"{dataset}_latest_rankings.csv"
        if not latest_path.exists():
            continue

        df = pd.read_csv(latest_path, low_memory=False)
        df["region_id"] = df["region_id"].astype(str)
        df["region_name"] = df["region_name"].astype(str)
        df = add_rank_percentile(df, "trend_score", ascending=False)

        for _, r in df.iterrows():
            rows.append({
                "dataset": dataset,
                "region_id": str(r["region_id"]),
                "region_name": str(r["region_name"]),
                "rank_overall": clean_value(r["rank_overall"]),
                "percentile": clean_value(r["percentile"]),
                "direction": clean_value(r["direction"]),
                "trend_label": clean_value(r.get("trend_label")),
                "yoy_pct_display": clean_value(r.get("yoy_pct_display")),
                "mom_pct_display": clean_value(r.get("mom_pct_display")),
            })

    write_json(DOCS / "profile_index.json", rows)
    print("Saved docs/data/profile_index.json")

def build_profile_payloads():
    nowcasts_path = V2 / "laus_nowcasts.csv"
    if nowcasts_path.exists():
        nowcasts = pd.read_csv(nowcasts_path, low_memory=False)
        nowcasts["region_id"] = nowcasts["region_id"].astype(str)
    else:
        nowcasts = pd.DataFrame()

    all_profiles = {}

    for dataset in DATASETS:
        latest_path = DERIVED / f"{dataset}_latest_rankings.csv"
        hist_path = DERIVED / f"{dataset}_region_month_metrics.csv"

        if not latest_path.exists() or not hist_path.exists():
            continue

        latest = pd.read_csv(latest_path, low_memory=False)
        hist = pd.read_csv(hist_path, low_memory=False)

        latest["region_id"] = latest["region_id"].astype(str)
        latest["region_name"] = latest["region_name"].astype(str)
        hist["region_id"] = hist["region_id"].astype(str)
        hist["date"] = hist["date"].astype(str)

        latest = add_rank_percentile(latest, "trend_score", ascending=False)

        if len(nowcasts) > 0:
            ds_now = nowcasts[nowcasts["dataset_name"] == dataset].copy()
            ds_now["region_id"] = ds_now["region_id"].astype(str)
            latest = latest.merge(
                ds_now[
                    [
                        "region_id",
                        "employment_yoy_nowcast",
                        "labor_force_yoy_nowcast",
                        "unemployment_rate_yoy_change_nowcast",
                        "confidence",
                        "industry_proxy",
                        "population_growth_proxy",
                    ]
                ],
                on="region_id",
                how="left",
            )

        payload = {}
        for region_id, sub in hist.groupby("region_id"):
            latest_row = latest[latest["region_id"] == region_id]
            if latest_row.empty:
                continue
            row = latest_row.iloc[0]

            payload[str(region_id)] = {
                "dataset": dataset,
                "region_id": str(region_id),
                "region_name": str(row["region_name"]),
                "rank_overall": clean_value(row.get("rank_overall")),
                "percentile": clean_value(row.get("percentile")),
                "direction": clean_value(row.get("direction")),
                "trend_label": clean_value(row.get("trend_label")),
                "trend_score": clean_value(row.get("trend_score")),
                "yoy_pct_display": clean_value(row.get("yoy_pct_display")),
                "mom_pct_display": clean_value(row.get("mom_pct_display")),
                "employment_yoy_nowcast": clean_value(row.get("employment_yoy_nowcast")),
                "labor_force_yoy_nowcast": clean_value(row.get("labor_force_yoy_nowcast")),
                "unemployment_rate_yoy_change_nowcast": clean_value(row.get("unemployment_rate_yoy_change_nowcast")),
                "confidence": clean_value(row.get("confidence")),
                "industry_proxy": clean_value(row.get("industry_proxy")),
                "population_growth_proxy": clean_value(row.get("population_growth_proxy")),
                "history": clean_records(
                    sub.sort_values("date").to_dict(orient="records")
                ),
            }

        all_profiles[dataset] = payload

    write_json(DOCS / "profiles.json", all_profiles)
    print("Saved docs/data/profiles.json")

def build_api_payload():
    api = {"datasets": {}}

    for dataset in DATASETS:
        latest_path = DERIVED / f"{dataset}_latest_rankings.csv"
        if not latest_path.exists():
            continue
        df = pd.read_csv(latest_path, low_memory=False)
        df["region_id"] = df["region_id"].astype(str)
        df = add_rank_percentile(df, "trend_score", ascending=False)

        api["datasets"][dataset] = {
            "top_by_trend": clean_records(df.head(50).to_dict(orient="records")),
            "bottom_by_trend": clean_records(df.sort_values("trend_score", ascending=True).head(50).to_dict(orient="records")),
        }

    write_json(DOCS / "api_regions.json", api)
    print("Saved docs/data/api_regions.json")

def build_premium_preview():
    payload = {
        "title": "Premium Preview",
        "features": [
            "Watchlists for counties, metros, and states",
            "Faster profile search",
            "Historical exports",
            "Full API access",
            "Alerts on ranking changes",
            "Nowcast downloads",
            "Report builder",
            "Weekly market notes"
        ]
    }
    write_json(DOCS / "premium_preview.json", payload)
    print("Saved docs/data/premium_preview.json")

def main():
    build_profile_index()
    build_profile_payloads()
    build_api_payload()
    build_premium_preview()

if __name__ == "__main__":
    main()