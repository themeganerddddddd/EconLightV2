from pathlib import Path
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.metrics import mean_absolute_error

V2 = Path("data/v2")
DOCS = Path("docs/data")
DOCS.mkdir(parents=True, exist_ok=True)

FEATURES = [
    "density_3m_smooth",
    "mom_pct_display",
    "yoy_pct_display",
    "mom_pct_rank",
    "yoy_pct_rank",
    "mom_3m_avg",
    "vol_12m",
    "trend_score",
    "months_seen",
]

TARGETS = {
    "employment_yoy_nowcast": "bls_employment_yoy_pct",
    "labor_force_yoy_nowcast": "bls_labor_force_yoy_pct",
    "unemployment_rate_yoy_change_nowcast": "bls_urate_yoy_change",
}

def clean_float(x):
    if pd.isna(x) or np.isinf(x):
        return None
    return float(x)

def train_dataset(dataset_name: str):
    path = V2 / f"{dataset_name}_panel.csv"
    if not path.exists():
        print(f"{dataset_name}: missing panel")
        return None

    df = pd.read_csv(path, low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df["region_id"] = df["region_id"].astype(str)

    results = []

    for out_name, target in TARGETS.items():
        train = df.dropna(subset=FEATURES + [target]).copy()
        if len(train) < 200:
            continue

        train = train.sort_values("date")
        cutoff = train["date"].quantile(0.8)
        tr = train[train["date"] <= cutoff]
        te = train[train["date"] > cutoff]

        if len(tr) < 100 or len(te) < 20:
            continue

        model = HistGradientBoostingRegressor(
            max_depth=4,
            learning_rate=0.05,
            max_iter=250,
            random_state=42
        )
        model.fit(tr[FEATURES], tr[target])

        pred = model.predict(te[FEATURES])
        mae = mean_absolute_error(te[target], pred)

        latest = df.sort_values("date").groupby("region_id", as_index=False).tail(1).copy()
        latest = latest.dropna(subset=FEATURES).copy()
        latest[out_name] = model.predict(latest[FEATURES])

        latest["dataset_name"] = dataset_name
        latest["target_name"] = out_name
        latest["model_mae"] = mae

        results.append(latest[
            [
                "date", "dataset_name", "region_id", "region_name",
                "trend_label", "trend_score",
                out_name, "model_mae"
            ]
        ])

    if not results:
        return None

    merged = results[0]
    for other in results[1:]:
        merged = merged.merge(
            other.drop(columns=["date", "dataset_name", "region_name", "trend_label", "trend_score"]),
            on=["region_id", "model_mae"],
            how="outer"
        )

    return results

def export_nowcasts():
    all_rows = []

    for dataset in ["states", "metros", "counties", "cities"]:
        res = train_dataset(dataset)
        if not res:
            continue
        for df in res:
            all_rows.append(df)

    if not all_rows:
        print("No nowcasts produced.")
        return

    all_nowcasts = pd.concat(all_rows, ignore_index=True)

    # collapse by dataset/region/date
    grouped = all_nowcasts.groupby(["dataset_name", "region_id", "region_name", "date"], as_index=False).agg({
        "trend_label": "first",
        "trend_score": "first",
        "employment_yoy_nowcast": "first",
        "labor_force_yoy_nowcast": "first",
        "unemployment_rate_yoy_change_nowcast": "first",
        "model_mae": "min",
    })

    grouped["confidence"] = np.where(
        grouped["model_mae"] <= grouped["model_mae"].quantile(0.33), "High",
        np.where(grouped["model_mae"] <= grouped["model_mae"].quantile(0.66), "Medium", "Low")
    )

    out_csv = V2 / "laus_nowcasts.csv"
    grouped.to_csv(out_csv, index=False)

    site_payload = {}
    for dataset, sub in grouped.groupby("dataset_name"):
        sub = sub.sort_values("employment_yoy_nowcast", ascending=False)
        site_payload[dataset] = {
            "top_employment_nowcasts": [
                {
                    "region_id": str(r["region_id"]),
                    "region_name": str(r["region_name"]),
                    "employment_yoy_nowcast": clean_float(r["employment_yoy_nowcast"]),
                    "labor_force_yoy_nowcast": clean_float(r["labor_force_yoy_nowcast"]),
                    "unemployment_rate_yoy_change_nowcast": clean_float(r["unemployment_rate_yoy_change_nowcast"]),
                    "trend_label": str(r["trend_label"]),
                    "confidence": str(r["confidence"]),
                }
                for _, r in sub.head(10).iterrows()
            ]
        }

    with open(DOCS / "v2_nowcasts.json", "w", encoding="utf-8") as f:
        json.dump(site_payload, f, indent=2, allow_nan=False)

    print(f"Saved {out_csv}")
    print("Saved docs/data/v2_nowcasts.json")

if __name__ == "__main__":
    export_nowcasts()