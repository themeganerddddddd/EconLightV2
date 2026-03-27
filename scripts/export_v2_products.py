from pathlib import Path
import json
import math
import numpy as np
import pandas as pd

DOCS = Path("docs/data")
DOCS.mkdir(parents=True, exist_ok=True)
V2 = Path("data/v2")

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

def main():
    nowcasts_path = V2 / "laus_nowcasts.csv"
    if not nowcasts_path.exists():
        print("Missing data/v2/laus_nowcasts.csv")
        return

    df = pd.read_csv(nowcasts_path, low_memory=False)
    df["region_id"] = df["region_id"].astype(str)
    if "date" in df.columns:
        df["date"] = df["date"].astype(str)

    api_payload = {
        "generated_from": "data/v2/laus_nowcasts.csv",
        "datasets": {}
    }

    for dataset, sub in df.groupby("dataset_name"):
        dataset_payload = {
            "latest_nowcasts": clean_records(sub.sort_values("employment_yoy_nowcast", ascending=False).head(50).to_dict(orient="records")),
            "best_population_growth_proxy": clean_records(
                sub.dropna(subset=["population_growth_proxy"])
                   .sort_values("population_growth_proxy", ascending=False)
                   .head(30)
                   .to_dict(orient="records")
            ),
            "highest_confidence": clean_records(
                sub.sort_values(["confidence", "employment_yoy_nowcast"], ascending=[True, False])
                   .head(30)
                   .to_dict(orient="records")
            ),
        }
        api_payload["datasets"][dataset] = dataset_payload

    write_json(DOCS / "api_nowcasts.json", api_payload)

    paid_payload = {
        "title": "Premium Preview",
        "features": [
            "Full nowcast downloads",
            "Historical exports",
            "Divergence alerts",
            "Custom watchlists",
            "API access",
            "Weekly market notes"
        ]
    }
    write_json(DOCS / "premium_preview.json", paid_payload)

    print("Saved docs/data/api_nowcasts.json")
    print("Saved docs/data/premium_preview.json")

if __name__ == "__main__":
    main()