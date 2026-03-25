from pathlib import Path
import os

from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
from blackmarble import BlackMarble

load_dotenv()

RAW_DIR = Path("data/raw")
DERIVED_DIR = Path("data/derived")

START = os.environ.get("DATA_START", "2024-01-01")
END = os.environ.get("DATA_END", "2026-02-01")
INCREMENTAL = os.environ.get("INCREMENTAL", "false").lower() == "true"

RAW_DIR.mkdir(parents=True, exist_ok=True)
DERIVED_DIR.mkdir(parents=True, exist_ok=True)


def run_dataset(dataset_name: str, regions_file: str):
    token = os.environ.get("BLACKMARBLE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BLACKMARBLE_TOKEN is not set.")

    regions_path = Path(regions_file)
    if not regions_path.exists():
        raise RuntimeError(f"Region file not found: {regions_path}")

    out_path = DERIVED_DIR / f"{dataset_name}_region_month.csv"

    gdf = gpd.read_file(regions_path).to_crs(4326)

    if "region_id" not in gdf.columns or "region_name" not in gdf.columns:
        raise RuntimeError(f"{regions_path} must include region_id and region_name properties.")

    area_gdf = gdf.to_crs(6933)
    gdf["area_km2"] = area_gdf.geometry.area / 1_000_000
    gdf["dataset_name"] = dataset_name

    if INCREMENTAL and out_path.exists():
        existing = pd.read_csv(out_path)
        existing["date"] = pd.to_datetime(existing["date"])
        last_date = existing["date"].max()
        start = (last_date + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
        dates = pd.date_range(start, END, freq="MS")
        print(f"{dataset_name}: incremental mode {start} to {END}")
        if len(dates) == 0:
            print(f"{dataset_name}: no new months to fetch")
            return
    else:
        dates = pd.date_range(START, END, freq="MS")
        print(f"{dataset_name}: full mode {START} to {END}")

    if len(dates) == 0:
        print(f"{dataset_name}: no dates generated")
        return

    bm = BlackMarble(
        token=token,
        collection="5200",
        output_directory=str(RAW_DIR),
        output_skip_if_exists=True,
    )

    try:
        df = bm.extract(gdf, "VNP46A3", dates)
    except ValueError as e:
        msg = str(e)
        if "Received an HTML response" in msg or "invalid or expired NASA Earthdata token" in msg:
            raise RuntimeError(
                "NASA returned an HTML login page instead of data. "
                "Generate a new Earthdata token and update your local .env or GitHub secret."
            ) from e
        raise RuntimeError(
            f"{dataset_name}: Black Marble manifest is missing one or more required files for this region set/date range.\n"
            f"Current regions file: {regions_path}\n"
            f"Current dates: {START} to {END}\n"
            f"Try a smaller geography or a later start date."
        ) from e

    df = df.rename(columns={c: c.lower() for c in df.columns})

    needed = {"region_id", "region_name", "date", "ntl_sum"}
    missing = needed - set(df.columns)
    if missing:
        raise RuntimeError(f"{dataset_name}: missing expected columns from bm.extract output: {missing}")

    if "area_km2" not in df.columns:
        df = df.merge(gdf[["region_id", "area_km2"]], on="region_id", how="left")

    df["date"] = pd.to_datetime(df["date"])
    df["ntl_sum"] = pd.to_numeric(df["ntl_sum"], errors="coerce")
    df["area_km2"] = pd.to_numeric(df["area_km2"], errors="coerce")
    df["light_density"] = df["ntl_sum"] / df["area_km2"]
    df["dataset_name"] = dataset_name

    new_data = df[
        [
            "date",
            "dataset_name",
            "region_id",
            "region_name",
            "ntl_sum",
            "area_km2",
            "light_density",
        ]
    ].dropna(subset=["date", "region_id", "region_name", "light_density"])

    if out_path.exists():
        old_data = pd.read_csv(out_path)
        old_data["date"] = pd.to_datetime(old_data["date"])
        combined = pd.concat([old_data, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "region_id"], keep="last")
    else:
        combined = new_data.copy()

    combined = combined.sort_values(["region_id", "date"]).reset_index(drop=True)
    combined.to_csv(out_path, index=False)
    print(f"{dataset_name}: saved {out_path} with {len(combined)} rows.")


def main():
    metros_file = os.environ.get("METROS_FILE", "data/regions/us_metros_top35.geojson")
    states_file = os.environ.get("STATES_FILE", "data/regions/us_states_contiguous.geojson")

    run_dataset("metros", metros_file)
    run_dataset("states", states_file)


if __name__ == "__main__":
    main()