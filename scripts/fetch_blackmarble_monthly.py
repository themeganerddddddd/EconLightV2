from pathlib import Path
import os

from dotenv import load_dotenv
import geopandas as gpd
import pandas as pd
from blackmarble import BlackMarble

load_dotenv()

REGIONS_PATH = Path("data/regions/us_test_regions.geojson")
RAW_DIR = Path("data/raw")
DERIVED_DIR = Path("data/derived")
OUT_PATH = DERIVED_DIR / "region_month.csv"

START = os.environ.get("DATA_START", "2022-01-01")
END = os.environ.get("DATA_END", "2026-02-01")

RAW_DIR.mkdir(parents=True, exist_ok=True)
DERIVED_DIR.mkdir(parents=True, exist_ok=True)


def main():
    token = os.environ.get("BLACKMARBLE_TOKEN")
    if not token:
        raise RuntimeError("BLACKMARBLE_TOKEN is not set.")

    gdf = gpd.read_file(REGIONS_PATH).to_crs(4326)

    if "region_id" not in gdf.columns or "region_name" not in gdf.columns:
        raise RuntimeError("GeoJSON must include region_id and region_name properties.")

    area_gdf = gdf.to_crs(6933)
    gdf["area_km2"] = area_gdf.geometry.area / 1_000_000

    dates = pd.date_range(START, END, freq="MS")
    if len(dates) == 0:
        raise RuntimeError("No monthly dates generated. Check DATA_START and DATA_END.")

    bm = BlackMarble(
        token=token,
        collection="5200",
        output_directory=str(RAW_DIR),
        output_skip_if_exists=True,
    )

    df = bm.extract(
        gdf,
        "VNP46A3",
        dates,
    )

    cols_lower = {c: c.lower() for c in df.columns}
    df = df.rename(columns=cols_lower)

    needed = {"region_id", "region_name", "date", "ntl_sum"}
    missing = needed - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing expected columns from bm.extract output: {missing}")

    if "area_km2" not in df.columns:
        df = df.merge(
            gdf[["region_id", "area_km2"]],
            on="region_id",
            how="left",
        )

    df["date"] = pd.to_datetime(df["date"])
    df["ntl_sum"] = pd.to_numeric(df["ntl_sum"], errors="coerce")
    df["area_km2"] = pd.to_numeric(df["area_km2"], errors="coerce")

    df["light_density"] = df["ntl_sum"] / df["area_km2"]

    out = df[
        [
            "date",
            "region_id",
            "region_name",
            "ntl_sum",
            "area_km2",
            "light_density",
        ]
    ].dropna(subset=["date", "region_id", "region_name", "light_density"])

    out = out.sort_values(["region_id", "date"]).reset_index(drop=True)
    out.to_csv(OUT_PATH, index=False)

    print(f"Saved {OUT_PATH} with {len(out)} rows.")


if __name__ == "__main__":
    main()