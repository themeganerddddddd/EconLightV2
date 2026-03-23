from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

DERIVED = Path("data/derived")
OUT = Path("outputs/social/monthly_summary.png")
OUT.parent.mkdir(parents=True, exist_ok=True)

index_df = pd.read_csv(DERIVED / "headline_index.csv")
latest = pd.read_csv(DERIVED / "latest_rankings.csv")

index_df["date"] = pd.to_datetime(index_df["date"])

top_up = latest.sort_values("trend_score", ascending=False).head(5)
top_down = latest.sort_values("trend_score", ascending=True).head(5)

fig = plt.figure(figsize=(12, 8))

ax = fig.add_axes([0.08, 0.48, 0.84, 0.42])
ax.plot(index_df["date"], index_df["index_level"])
ax.set_title("Econ Light Monitor — Headline Index")
ax.set_ylabel("Index Level")
ax.grid(True, alpha=0.3)

fig.text(0.08, 0.38, "Top Brightening Regions", fontsize=14, weight="bold")
for i, (_, row) in enumerate(top_up.iterrows(), start=1):
    yoy = row["yoy_pct"] * 100 if pd.notna(row["yoy_pct"]) else 0
    fig.text(0.08, 0.38 - i * 0.04, f"{i}. {row['region_name']}  ({yoy:.1f}% YoY)")

fig.text(0.55, 0.38, "Top Dimming Regions", fontsize=14, weight="bold")
for i, (_, row) in enumerate(top_down.iterrows(), start=1):
    yoy = row["yoy_pct"] * 100 if pd.notna(row["yoy_pct"]) else 0
    fig.text(0.55, 0.38 - i * 0.04, f"{i}. {row['region_name']}  ({yoy:.1f}% YoY)")

latest_idx = index_df.iloc[-1]["index_level"]
fig.text(0.08, 0.08, f"Current Headline Index: {latest_idx:.1f}", fontsize=16, weight="bold")
fig.text(0.08, 0.04, "Source: NASA Black Marble VNP46A3", fontsize=10)

plt.savefig(OUT, dpi=200, bbox_inches="tight")
print(f"Saved {OUT}")