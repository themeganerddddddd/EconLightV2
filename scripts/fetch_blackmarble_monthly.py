from pathlib import Path
import os

from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
from blackmarble import BlackMarble

load_dotenv()

RAW_DIR = Path("data/raw")
DERIVED_DIR = Path("data/derived")

START = os.environ.get("DATA_START", "2022-01-01")
END = os.environ.get("DATA_END", "2026-02-01")
INCREMENTAL = os.environ.get("INCREMENTAL", "false").lower() == "true"

RAW_DIR.mkdir(parents=True, exist_ok=True)
DERIVED_DIR.mkdir(parents=True, exist_ok=True)


def fetch_one_dataset(dataset_name: str, regions_file: str):
    token = os.environ.get("BLACKMARBLE_TOKEN", "").strip()
    if not token:
        raise RuntimeError("BLACKMARBLE_TOKEN is not set.")

    regions_path = Path(regions_file)
    if not regions_path.exists():
        raise RuntimeError(f"Region file not found: {regions_path}")

    out_path = DERIVED_DIR / f"{dataset_name}_region_month.csv"

    gdf = gpd.read_file(regions_path).to_crs(4326)
    if "region_id" not in gdf.columns or "region_name" not in gdf.columns:
        raise RuntimeError(f"{regions_path} must include region_id and region_name.")

    area_gdf = gdf.to_crs(6933)
    gdf["area_km2"] = area_gdf.geometry.area / 1_000_000
    gdf["dataset_name"] = dataset_name

    if INCREMENTAL and out_path.exists():
        existing = pd.read_csv(out_path)
        existing["date"] = pd.to_datetime(existing["date"])
        start = (existing["date"].max() + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
        dates = pd.date_range(start, END, freq="MS")
        print(f"{dataset_name}: incremental mode {start} to {END}")
    else:
        dates = pd.date_range(START, END, freq="MS")
        print(f"{dataset_name}: full mode {START} to {END}")

    if len(dates) == 0:
        print(f"{dataset_name}: no new months")
        return

    bm = BlackMarble(
        token=token,
        collection="5200",
        output_directory=str(RAW_DIR),
        output_skip_if_exists=True,
    )

    frames = []

    for d in dates:
        try:
            month_df = bm.extract(gdf, "VNP46A3", pd.DatetimeIndex([d]))
            month_df = month_df.rename(columns={c: c.lower() for c in month_df.columns})

            needed = {"region_id", "region_name", "date", "ntl_sum"}
            missing = needed - set(month_df.columns)
            if missing:
                print(f"{dataset_name}: skip {d.date()} missing columns {missing}")
                continue

            if "area_km2" not in month_df.columns:
                month_df = month_df.merge(gdf[["region_id", "area_km2"]], on="region_id", how="left")

            month_df["date"] = pd.to_datetime(month_df["date"])
            month_df["ntl_sum"] = pd.to_numeric(month_df["ntl_sum"], errors="coerce")
            month_df["area_km2"] = pd.to_numeric(month_df["area_km2"], errors="coerce")
            month_df["light_density"] = month_df["ntl_sum"] / month_df["area_km2"]
            month_df["dataset_name"] = dataset_name

            month_df = month_df[
                ["date", "dataset_name", "region_id", "region_name", "ntl_sum", "area_km2", "light_density"]
            ].dropna(subset=["date", "region_id", "region_name", "light_density"])

            frames.append(month_df)
            print(f"{dataset_name}: fetched {d.strftime('%Y-%m')}")
        except Exception as e:
            print(f"{dataset_name}: skipped {d.strftime('%Y-%m')} because {e}")

    if not frames:
        print(f"{dataset_name}: no data fetched")
        return

    new_data = pd.concat(frames, ignore_index=True)

    if out_path.exists():
        old_data = pd.read_csv(out_path)
        old_data["date"] = pd.to_datetime(old_data["date"])
        combined = pd.concat([old_data, new_data], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "region_id"], keep="last")
    else:
        combined = new_data.copy()

    combined = combined.sort_values(["region_id", "date"]).reset_index(drop=True)
    combined.to_csv(out_path, index=False)
    print(f"{dataset_name}: saved {out_path} with {len(combined)} rows")


def main():
    fetch_one_dataset("metros", os.environ.get("METROS_FILE", "data/regions/us_metros_all.geojson"))
    fetch_one_dataset("states", os.environ.get("STATES_FILE", "data/regions/us_states_contiguous.geojson"))
    fetch_one_dataset("counties", os.environ.get("COUNTIES_FILE", "data/regions/us_counties_contiguous.geojson"))
    fetch_one_dataset("cities", os.environ.get("CITIES_FILE", "data/regions/us_cities_top200.geojson"))


if __name__ == "__main__":
    main()