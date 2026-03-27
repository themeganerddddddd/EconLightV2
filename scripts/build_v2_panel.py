from pathlib import Path
import pandas as pd
import numpy as np

DERIVED = Path("data/derived")
BLS = Path("data/bls")
V2 = Path("data/v2")
V2.mkdir(parents=True, exist_ok=True)

BLS_COLS = [
    "bls_unemployment_rate",
    "bls_unemployment",
    "bls_employment",
    "bls_labor_force",
]

def build_dataset(dataset_name: str):
    lights_path = DERIVED / f"{dataset_name}_region_month_metrics.csv"
    bls_path = BLS / f"{dataset_name}_laus_monthly.csv"

    if not lights_path.exists():
        print(f"{dataset_name}: missing lights input, skipping")
        return

    lights = pd.read_csv(lights_path, low_memory=False)
    lights["date"] = pd.to_datetime(lights["date"])
    lights["region_id"] = lights["region_id"].astype(str)

    if not bls_path.exists():
        print(f"{dataset_name}: missing BLS file, writing lights-only panel")
        df = lights.copy()
        for col in BLS_COLS:
            df[col] = np.nan
    else:
        bls = pd.read_csv(bls_path, low_memory=False)
        bls["date"] = pd.to_datetime(bls["date"])
        bls["region_id"] = bls["region_id"].astype(str)

        for col in BLS_COLS:
            if col not in bls.columns:
                bls[col] = np.nan

        df = lights.merge(
            bls[
                ["date", "region_id"] + BLS_COLS
            ],
            on=["date", "region_id"],
            how="left"
        )

    g = df.groupby("region_id", group_keys=False)

    df["bls_employment_lag12"] = g["bls_employment"].shift(12)
    df["bls_labor_force_lag12"] = g["bls_labor_force"].shift(12)
    df["bls_unemployment_rate_lag12"] = g["bls_unemployment_rate"].shift(12)

    df["bls_employment_yoy_pct"] = np.where(
        df["bls_employment_lag12"] > 0,
        (df["bls_employment"] - df["bls_employment_lag12"]) / df["bls_employment_lag12"],
        np.nan
    )
    df["bls_labor_force_yoy_pct"] = np.where(
        df["bls_labor_force_lag12"] > 0,
        (df["bls_labor_force"] - df["bls_labor_force_lag12"]) / df["bls_labor_force_lag12"],
        np.nan
    )
    df["bls_urate_yoy_change"] = df["bls_unemployment_rate"] - df["bls_unemployment_rate_lag12"]

    out = V2 / f"{dataset_name}_panel.csv"
    df.to_csv(out, index=False)
    print(f"Saved {out} with {len(df)} rows.")


def main():
    for dataset in ["states", "metros", "counties", "cities"]:
        build_dataset(dataset)

if __name__ == "__main__":
    main()