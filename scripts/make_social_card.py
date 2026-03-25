from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd

DERIVED = Path("data/derived")
OUT = Path("outputs/social/monthly_summary.png")
OUT.parent.mkdir(parents=True, exist_ok=True)

index_path = DERIVED / "metros_headline_index.csv"
latest_path = DERIVED / "metros_latest_rankings.csv"

if not index_path.exists() or not latest_path.exists():
    print("metros files missing, skipping social card")
    raise SystemExit(0)

index_df = pd.read_csv(index_path)
latest = pd.read_csv(latest_path)

index_df["date"] = pd.to_datetime(index_df["date"])

top_up = latest.sort_values("trend_score", ascending=False).head(5)
top_down = latest.sort_values("trend_score", ascending=True).head(5)

fig = plt.figure(figsize=(12, 8))
fig.patch.set_facecolor("#0a1022")

ax = fig.add_axes([0.08, 0.48, 0.84, 0.42])
ax.set_facecolor("#10182f")
ax.plot(index_df["date"], index_df["index_level"], linewidth=2)
ax.set_title("Econ Light Monitor — Metro Headline Index", color="white")
ax.set_ylabel("Index Level", color="white")
ax.tick_params(colors="white")
for spine in ax.spines.values():
    spine.set_color("#334155")
ax.grid(True, alpha=0.25)

fig.text(0.08, 0.38, "Top Brightening Metros", fontsize=14, weight="bold", color="white")
for i, (_, row) in enumerate(top_up.iterrows(), start=1):
    yoy = row["yoy_pct"] * 100 if pd.notna(row["yoy_pct"]) else 0
    fig.text(0.08, 0.38 - i * 0.04, f"{i}. {row['region_name']}  ({yoy:.1f}% YoY)", color="white")

fig.text(0.55, 0.38, "Top Dimming Metros", fontsize=14, weight="bold", color="white")
for i, (_, row) in enumerate(top_down.iterrows(), start=1):
    yoy = row["yoy_pct"] * 100 if pd.notna(row["yoy_pct"]) else 0
    fig.text(0.55, 0.38 - i * 0.04, f"{i}. {row['region_name']}  ({yoy:.1f}% YoY)", color="white")

latest_idx = index_df.iloc[-1]["index_level"]
fig.text(0.08, 0.08, f"Current Metro Index: {latest_idx:.1f}", fontsize=16, weight="bold", color="white")
fig.text(0.08, 0.04, "Source: NASA Black Marble VNP46A3", fontsize=10, color="#cbd5e1")

plt.savefig(OUT, dpi=200, bbox_inches="tight", facecolor=fig.get_facecolor())
print(f"Saved {OUT}")