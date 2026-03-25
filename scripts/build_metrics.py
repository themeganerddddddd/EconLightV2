from pathlib import Path
import numpy as np
import pandas as pd

DERIVED_DIR = Path("data/derived")
DERIVED_DIR.mkdir(parents=True, exist_ok=True)


def classify_trend(score):
    if pd.isna(score):
        return "Insufficient data"
    if score >= 1.25:
        return "Very strong"
    if score >= 0.5:
        return "Strong"
    if score >= -0.5:
        return "Stable"
    if score >= -1.25:
        return "Weak"
    return "Very weak"


def build_for_dataset(dataset_name: str):
    in_path = DERIVED_DIR / f"{dataset_name}_region_month.csv"
    if not in_path.exists():
        print(f"{dataset_name}: input file not found, skipping")
        return

    df = pd.read_csv(in_path, low_memory=False)
    df["date"] = pd.to_datetime(df["date"])
    df["region_id"] = df["region_id"].astype(str)
    df = df.sort_values(["region_id", "date"]).reset_index(drop=True)

    g = df.groupby("region_id", group_keys=False)

    # Stable signal used for display and comparison
    df["density_3m_smooth"] = g["light_density"].transform(
        lambda s: s.rolling(3, min_periods=1).mean()
    )

    df["lag_1"] = g["density_3m_smooth"].shift(1)
    df["lag_12"] = g["density_3m_smooth"].shift(12)

    # Display percentages come directly from the displayed smoothed density
    df["mom_pct_display"] = np.where(
        df["lag_1"] > 0,
        (df["density_3m_smooth"] - df["lag_1"]) / df["lag_1"],
        np.nan,
    )
    df["yoy_pct_display"] = np.where(
        df["lag_12"] > 0,
        (df["density_3m_smooth"] - df["lag_12"]) / df["lag_12"],
        np.nan,
    )

    # Rank versions are clipped for stability
    cap = 0.5
    df["mom_capped"] = df["mom_pct_display"].abs() >= cap
    df["yoy_capped"] = df["yoy_pct_display"].abs() >= cap

    df["mom_pct_rank"] = df["mom_pct_display"].clip(-cap, cap)
    df["yoy_pct_rank"] = df["yoy_pct_display"].clip(-cap, cap)

    df["mom_3m_avg"] = g["mom_pct_rank"].transform(
        lambda s: s.rolling(3, min_periods=1).mean()
    )
    df["vol_12m"] = g["mom_pct_rank"].transform(
        lambda s: s.rolling(12, min_periods=6).std()
    )
    df["months_seen"] = g.cumcount() + 1

    for col in ["mom_pct_rank", "yoy_pct_rank", "mom_3m_avg"]:
        mean = df[col].mean(skipna=True)
        std = df[col].std(skipna=True)
        if std and std > 0:
            df[f"{col}_z"] = (df[col] - mean) / std
        else:
            df[f"{col}_z"] = 0.0

    df["trend_score"] = (
        0.50 * df["yoy_pct_rank_z"].fillna(0)
        + 0.30 * df["mom_pct_rank_z"].fillna(0)
        + 0.20 * df["mom_3m_avg_z"].fillna(0)
    )
    df["trend_label"] = df["trend_score"].apply(classify_trend)

    latest_date = df["date"].max()
    latest = df[df["date"] == latest_date].copy()
    latest = latest[latest["months_seen"] >= 6].copy()
    latest["rankable"] = latest["months_seen"] >= 12

    latest_rankings = latest[
        [
            "date",
            "dataset_name",
            "region_id",
            "region_name",
            "ntl_sum",
            "light_density",
            "density_3m_smooth",
            "mom_pct_display",
            "yoy_pct_display",
            "mom_pct_rank",
            "yoy_pct_rank",
            "mom_capped",
            "yoy_capped",
            "mom_3m_avg",
            "vol_12m",
            "trend_score",
            "trend_label",
            "months_seen",
            "rankable",
        ]
    ].sort_values(
        ["rankable", "trend_score", "region_name"],
        ascending=[False, False, True]
    )

    leaders = latest_rankings.head(20).copy()
    laggards = latest_rankings.sort_values(
        ["rankable", "trend_score", "region_name"],
        ascending=[False, True, True]
    ).head(20).copy()

    # National / dataset-level index
    index_df = (
        df.groupby("date", as_index=False)
        .agg(
            avg_density=("density_3m_smooth", "mean"),
            avg_yoy=("yoy_pct_display", "mean"),
            avg_mom=("mom_pct_display", "mean"),
            total_ntl=("ntl_sum", "sum"),
        )
        .sort_values("date")
    )

    index_df["total_ntl_lag_12"] = index_df["total_ntl"].shift(12)
    index_df["national_yoy_pct"] = np.where(
        index_df["total_ntl_lag_12"] > 0,
        (index_df["total_ntl"] - index_df["total_ntl_lag_12"]) / index_df["total_ntl_lag_12"],
        np.nan,
    )

    levels = [100.0]
    for i in range(1, len(index_df)):
        prev = levels[-1]
        growth = index_df.iloc[i]["avg_mom"]
        growth = 0 if pd.isna(growth) else growth
        levels.append(prev * (1 + growth))
    index_df["index_level"] = levels
    index_df["dataset_name"] = dataset_name

    df.to_csv(DERIVED_DIR / f"{dataset_name}_region_month_metrics.csv", index=False)
    latest_rankings.to_csv(DERIVED_DIR / f"{dataset_name}_latest_rankings.csv", index=False)
    leaders.to_csv(DERIVED_DIR / f"{dataset_name}_leaders.csv", index=False)
    laggards.to_csv(DERIVED_DIR / f"{dataset_name}_laggards.csv", index=False)
    index_df.to_csv(DERIVED_DIR / f"{dataset_name}_headline_index.csv", index=False)

    print(f"{dataset_name}: saved metric outputs")


def main():
    for dataset in ["metros", "states", "counties", "cities"]:
        build_for_dataset(dataset)


if __name__ == "__main__":
    main()