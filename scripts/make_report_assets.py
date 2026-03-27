from pathlib import Path
import json
import pandas as pd
import matplotlib.pyplot as plt

DOCS = Path("docs/data")
OUT = Path("docs/assets")
OUT.mkdir(parents=True, exist_ok=True)

def main():
    path = DOCS / "v2_nowcasts.json"
    if not path.exists():
        print("Missing docs/data/v2_nowcasts.json")
        return

    with open(path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    rows = payload.get("metros", {}).get("top_employment_nowcasts", [])[:10]
    if not rows:
        print("No metro nowcasts found.")
        return

    df = pd.DataFrame(rows)
    df = df.sort_values("employment_yoy_nowcast", ascending=True)

    plt.figure(figsize=(10, 6))
    plt.barh(df["region_name"], df["employment_yoy_nowcast"])
    plt.xlabel("Implied employment YoY")
    plt.ylabel("Metro")
    plt.title("Top Metro Employment Nowcasts")
    plt.tight_layout()
    plt.savefig(OUT / "top_metro_nowcasts.png", dpi=180)
    plt.close()

    markdown = "# Econ Light Monitor Update\n\n"
    markdown += "## Top Metro Employment Nowcasts\n\n"
    for _, r in df.sort_values("employment_yoy_nowcast", ascending=False).iterrows():
        markdown += f"- **{r['region_name']}**: implied employment YoY {r['employment_yoy_nowcast']:.1%}, confidence {r['confidence']}\n"

    (OUT / "substack_update.md").write_text(markdown, encoding="utf-8")

    tweet = "Top metro employment nowcasts from Econ Light Monitor:\n"
    for _, r in df.sort_values("employment_yoy_nowcast", ascending=False).head(5).iterrows():
        tweet += f"{r['region_name']}: {r['employment_yoy_nowcast']:.1%}\n"
    tweet += "\nBuilt from nighttime lights + BLS labor data."

    (OUT / "twitter_post.txt").write_text(tweet, encoding="utf-8")

    print("Saved docs/assets/top_metro_nowcasts.png")
    print("Saved docs/assets/substack_update.md")
    print("Saved docs/assets/twitter_post.txt")

if __name__ == "__main__":
    main()