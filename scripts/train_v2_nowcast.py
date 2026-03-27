from pathlib import Path
import json
import math
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

CORE_DATASETS = ["states", "metros", "counties"]


def clean_float(x):
    if pd.isna(x) or np.isinf(x):
        return None
    return float(x)


def classify_confidence(mae, series):
    if pd.isna(mae):
        return "Low"
    q1 = series.quantile(0.33)
    q2 = series.quantile(0.66)
    if mae <= q1:
        return "High"
    if mae <= q2:
        return "Medium"
    return "Low"


def industry_proxy(row):
    emp = row.get("employment_yoy_nowcast")
    lf = row.get("labor_force_yoy_nowcast")
    ur = row.get("unemployment_rate_yoy_change_nowcast")
    trend = row.get("trend_score")
    vol = row.get("vol_12m")

    if pd.notna(emp) and emp > 0.03 and pd.notna(lf) and lf > 0.02:
        return "Broad expansion"
    if pd.notna(trend) and trend > 1.0 and pd.notna(vol) and vol < 0.05:
        return "Steady business growth"
    if pd.notna(trend) and trend > 0.8 and pd.notna(vol) and vol >= 0.05:
        return "Construction or cyclical growth"
    if pd.notna(emp) and emp < -0.02 and pd.notna(ur) and ur > 0.2:
        return "Labor softening"
    if pd.notna(trend) and trend < -0.8:
        return "Weak local demand"
    return "Mixed signal"


def population_proxy(row):
    lf = row.get("labor_force_yoy_nowcast")
    emp = row.get("employment_yoy_nowcast")
    yoy_light = row.get("yoy_pct_display")
    trend = row.get("trend_score")

    vals = [v for v in [lf, emp, yoy_light] if pd.notna(v)]
    if not vals:
        return None

    score = 0.0
    if pd.notna(lf):
        score += 0.45 * lf
    if pd.notna(emp):
        score += 0.35 * emp
    if pd.notna(yoy_light):
        score += 0.20 * yoy_light

    if pd.notna(trend):
        score += 0.01 * trend

    return float(score)


def train_dataset(dataset_name: str):
    path = V2 / f"{dataset_name}_panel.csv"
    if not path.exists():
        print(f"{dataset_name}: missing panel")
        return None

    df = pd.read_csv(path, low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df["region_id"] = df["region_id"].astype(str)

    model_outputs = []

    for out_name, target in TARGETS.items():
        train = df.dropna(subset=FEATURES + [target]).copy()
        if len(train) < 200:
            print(f"{dataset_name}: insufficient rows for {out_name}")
            continue

        train = train.sort_values("date")
        cutoff = train["date"].quantile(0.8)
        tr = train[train["date"] <= cutoff]
        te = train[train["date"] > cutoff]

        if len(tr) < 100 or len(te) < 20:
            print(f"{dataset_name}: insufficient split for {out_name}")
            continue

        model = HistGradientBoostingRegressor(
            max_depth=4,
            learning_rate=0.05,
            max_iter=250,
            random_state=42,
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

        keep_cols = [
            "date",
            "dataset_name",
            "region_id",
            "region_name",
            "trend_label",
            "trend_score",
            "yoy_pct_display",
            "mom_pct_display",
            "vol_12m",
            out_name,
            "model_mae",
        ]
        model_outputs.append(latest[keep_cols])

    if not model_outputs:
        return None

    merged = None
    for df_part in model_outputs:
        cols = [c for c in df_part.columns if c not in ["trend_label", "trend_score", "yoy_pct_display", "mom_pct_display", "vol_12m", "model_mae"]]
        base_cols = ["date", "dataset_name", "region_id", "region_name", "trend_label", "trend_score", "yoy_pct_display", "mom_pct_display", "vol_12m", "model_mae"]
        if merged is None:
            merged = df_part.copy()
        else:
            merged = merged.merge(
                df_part[[c for c in cols if c not in ["date", "dataset_name", "region_id", "region_name"]]],
                left_index=True,
                right_index=True,
                how="outer",
            )

    return merged


def export_nowcasts():
    all_rows = []

    for dataset in CORE_DATASETS + ["cities"]:
        res = train_dataset(dataset)
        if res is not None and len(res) > 0:
            all_rows.append(res)

    if not all_rows:
        print("No nowcasts produced.")
        return

    grouped = pd.concat(all_rows, ignore_index=True)

    grouped["confidence"] = grouped["model_mae"].apply(lambda x: classify_confidence(x, grouped["model_mae"]))
    grouped["industry_proxy"] = grouped.apply(industry_proxy, axis=1)
    grouped["population_growth_proxy"] = grouped.apply(population_proxy, axis=1)

    grouped["divergence_score"] = np.where(
        grouped["employment_yoy_nowcast"].notna() & grouped["yoy_pct_display"].notna(),
        grouped["employment_yoy_nowcast"] - grouped["yoy_pct_display"],
        np.nan,
    )

    out_csv = V2 / "laus_nowcasts.csv"
    grouped.to_csv(out_csv, index=False)

    site_payload = {}
    divergence_payload = {}

    for dataset, sub in grouped.groupby("dataset_name"):
        sub = sub.sort_values("employment_yoy_nowcast", ascending=False)

        top_nowcasts = []
        for _, r in sub.head(20).iterrows():
            top_nowcasts.append({
                "region_id": str(r["region_id"]),
                "region_name": str(r["region_name"]),
                "employment_yoy_nowcast": clean_float(r.get("employment_yoy_nowcast")),
                "labor_force_yoy_nowcast": clean_float(r.get("labor_force_yoy_nowcast")),
                "unemployment_rate_yoy_change_nowcast": clean_float(r.get("unemployment_rate_yoy_change_nowcast")),
                "trend_label": str(r.get("trend_label")),
                "trend_score": clean_float(r.get("trend_score")),
                "yoy_pct_display": clean_float(r.get("yoy_pct_display")),
                "mom_pct_display": clean_float(r.get("mom_pct_display")),
                "confidence": str(r.get("confidence")),
                "industry_proxy": r.get("industry_proxy"),
                "population_growth_proxy": clean_float(r.get("population_growth_proxy")),
            })

        bottom_nowcasts = []
        for _, r in sub.sort_values("employment_yoy_nowcast", ascending=True).head(20).iterrows():
            bottom_nowcasts.append({
                "region_id": str(r["region_id"]),
                "region_name": str(r["region_name"]),
                "employment_yoy_nowcast": clean_float(r.get("employment_yoy_nowcast")),
                "labor_force_yoy_nowcast": clean_float(r.get("labor_force_yoy_nowcast")),
                "unemployment_rate_yoy_change_nowcast": clean_float(r.get("unemployment_rate_yoy_change_nowcast")),
                "trend_label": str(r.get("trend_label")),
                "trend_score": clean_float(r.get("trend_score")),
                "confidence": str(r.get("confidence")),
                "industry_proxy": r.get("industry_proxy"),
                "population_growth_proxy": clean_float(r.get("population_growth_proxy")),
            })

        site_payload[dataset] = {
            "top_employment_nowcasts": top_nowcasts,
            "bottom_employment_nowcasts": bottom_nowcasts,
        }

        div = sub.dropna(subset=["divergence_score"]).copy()
        divergence_payload[dataset] = {
            "top_positive_divergence": [
                {
                    "region_id": str(r["region_id"]),
                    "region_name": str(r["region_name"]),
                    "divergence_score": clean_float(r["divergence_score"]),
                    "employment_yoy_nowcast": clean_float(r["employment_yoy_nowcast"]),
                    "yoy_pct_display": clean_float(r["yoy_pct_display"]),
                    "confidence": str(r["confidence"]),
                }
                for _, r in div.sort_values("divergence_score", ascending=False).head(15).iterrows()
            ],
            "top_negative_divergence": [
                {
                    "region_id": str(r["region_id"]),
                    "region_name": str(r["region_name"]),
                    "divergence_score": clean_float(r["divergence_score"]),
                    "employment_yoy_nowcast": clean_float(r["employment_yoy_nowcast"]),
                    "yoy_pct_display": clean_float(r["yoy_pct_display"]),
                    "confidence": str(r["confidence"]),
                }
                for _, r in div.sort_values("divergence_score", ascending=True).head(15).iterrows()
            ],
        }

    with open(DOCS / "v2_nowcasts.json", "w", encoding="utf-8") as f:
        json.dump(site_payload, f, indent=2, allow_nan=False)

    with open(DOCS / "v2_divergence.json", "w", encoding="utf-8") as f:
        json.dump(divergence_payload, f, indent=2, allow_nan=False)

    print(f"Saved {out_csv}")
    print("Saved docs/data/v2_nowcasts.json")
    print("Saved docs/data/v2_divergence.json")


if __name__ == "__main__":
    export_nowcasts()