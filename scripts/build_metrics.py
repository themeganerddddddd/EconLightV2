from pathlib import Path
import numpy as np
import pandas as pd

IN = Path("data/derived/region_month.csv")
OUT_DIR = Path("data/derived")
OUT_DIR.mkdir(parents=True, exist_ok=True)

df = pd.read_csv(IN)
df["date"] = pd.to_datetime(df["date"])
df = df.sort_values(["region_id", "date"]).reset_index(drop=True)

metric_col = "light_density"
g = df.groupby("region_id", group_keys=False)

df["lag_1"] = g[metric_col].shift(1)
df["lag_3"] = g[metric_col].shift(3)
df["lag_12"] = g[metric_col].shift(12)

df["mom_pct"] = np.where(
    df["lag_1"] > 0,
    (df[metric_col] - df["lag_1"]) / df["lag_1"],
    np.nan,
)

df["yoy_pct"] = np.where(
    df["lag_12"] > 0,
    (df[metric_col] - df["lag_12"]) / df["lag_12"],
    np.nan,
)

df["mom_3m_avg"] = g["mom_pct"].transform(lambda s: s.rolling(3).mean())
df["yoy_3m_avg"] = g["yoy_pct"].transform(lambda s: s.rolling(3).mean())
df["vol_12m"] = g["mom_pct"].transform(lambda s: s.rolling(12).std())

for col in ["mom_pct", "yoy_pct", "mom_3m_avg"]:
    mean = df[col].mean(skipna=True)
    std = df[col].std(skipna=True)
    if std and std > 0:
        df[f"{col}_z"] = (df[col] - mean) / std
    else:
        df[f"{col}_z"] = 0

df["trend_score"] = (
    0.5 * df["yoy_pct_z"].fillna(0)
    + 0.3 * df["mom_pct_z"].fillna(0)
    + 0.2 * df["mom_3m_avg_z"].fillna(0)
)

latest_date = df["date"].max()
latest = df[df["date"] == latest_date].copy()

latest_rankings = latest[
    [
        "date",
        "region_id",
        "region_name",
        "ntl_sum",
        "light_density",
        "mom_pct",
        "yoy_pct",
        "mom_3m_avg",
        "vol_12m",
        "trend_score",
    ]
].sort_values("trend_score", ascending=False)

leaders = latest_rankings.head(10).copy()
laggards = latest_rankings.sort_values("trend_score", ascending=True).head(10).copy()

index_df = (
    df.groupby("date", as_index=False)
    .agg(
        avg_density=("light_density", "mean"),
        avg_yoy=("yoy_pct", "mean"),
        avg_mom=("mom_pct", "mean"),
    )
    .sort_values("date")
)

levels = [100.0]
for i in range(1, len(index_df)):
    prev = levels[-1]
    growth = index_df.iloc[i]["avg_mom"]
    growth = 0 if pd.isna(growth) else growth
    levels.append(prev * (1 + growth))

index_df["index_level"] = levels

df.to_csv(OUT_DIR / "region_month_metrics.csv", index=False)
latest_rankings.to_csv(OUT_DIR / "latest_rankings.csv", index=False)
leaders.to_csv(OUT_DIR / "leaders.csv", index=False)
laggards.to_csv(OUT_DIR / "laggards.csv", index=False)
index_df.to_csv(OUT_DIR / "headline_index.csv", index=False)

print("Saved metric outputs.")