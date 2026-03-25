from pathlib import Path
import requests
import geopandas as gpd
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

REGIONS_DIR = Path("data/regions")
TMP_DIR = Path("data/tmp")
REGIONS_DIR.mkdir(parents=True, exist_ok=True)
TMP_DIR.mkdir(parents=True, exist_ok=True)

ACS_YEAR = os.environ.get("ACS_YEAR", "2022")
CARTO_YEAR = os.environ.get("CARTO_YEAR", "2024")
CITIES_TOP_N = int(os.environ.get("CITIES_TOP_N", "200"))

CBSA_URL = f"https://www2.census.gov/geo/tiger/GENZ{CARTO_YEAR}/shp/cb_{CARTO_YEAR}_us_cbsa_500k.zip"
COUNTY_URL = f"https://www2.census.gov/geo/tiger/GENZ{CARTO_YEAR}/shp/cb_{CARTO_YEAR}_us_county_500k.zip"
STATE_URL = f"https://www2.census.gov/geo/tiger/GENZ{CARTO_YEAR}/shp/cb_{CARTO_YEAR}_us_state_500k.zip"
PLACE_URL = f"https://www2.census.gov/geo/tiger/GENZ{CARTO_YEAR}/shp/cb_{CARTO_YEAR}_us_place_500k.zip"

STATE_EXCLUDE = {"02", "15", "60", "66", "69", "72", "78"}  # AK, HI, territories


def download(url: str, out_path: Path):
    if out_path.exists():
        return out_path
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    out_path.write_bytes(r.content)
    return out_path


def read_zipped_shapefile(zip_path: Path) -> gpd.GeoDataFrame:
    return gpd.read_file(f"zip://{zip_path.resolve()}")


def census_json(url: str) -> pd.DataFrame:
    r = requests.get(url, timeout=120)
    r.raise_for_status()
    data = r.json()
    return pd.DataFrame(data[1:], columns=data[0])


def build_states():
    zip_path = download(STATE_URL, TMP_DIR / Path(STATE_URL).name)
    gdf = read_zipped_shapefile(zip_path).to_crs(4326)

    gdf = gdf[~gdf["STATEFP"].isin(STATE_EXCLUDE)].copy()

    out = gdf[["STATEFP", "NAME", "geometry"]].copy()
    out = out.rename(columns={"STATEFP": "region_id", "NAME": "region_name"})
    out.to_file(REGIONS_DIR / "us_states_contiguous.geojson", driver="GeoJSON")
    print("Saved us_states_contiguous.geojson")


def build_counties():
    zip_path = download(COUNTY_URL, TMP_DIR / Path(COUNTY_URL).name)
    gdf = read_zipped_shapefile(zip_path).to_crs(4326)

    gdf = gdf[~gdf["STATEFP"].isin(STATE_EXCLUDE)].copy()

    # COUNTYNS/NAME/STATE_NAME vary a bit by file vintage; GEOID is stable.
    if "STATE_NAME" not in gdf.columns:
        state_map = {
            "01": "Alabama","04": "Arizona","05": "Arkansas","06": "California","08": "Colorado","09": "Connecticut",
            "10": "Delaware","12": "Florida","13": "Georgia","16": "Idaho","17": "Illinois","18": "Indiana",
            "19": "Iowa","20": "Kansas","21": "Kentucky","22": "Louisiana","23": "Maine","24": "Maryland",
            "25": "Massachusetts","26": "Michigan","27": "Minnesota","28": "Mississippi","29": "Missouri",
            "30": "Montana","31": "Nebraska","32": "Nevada","33": "New Hampshire","34": "New Jersey",
            "35": "New Mexico","36": "New York","37": "North Carolina","38": "North Dakota","39": "Ohio",
            "40": "Oklahoma","41": "Oregon","42": "Pennsylvania","44": "Rhode Island","45": "South Carolina",
            "46": "South Dakota","47": "Tennessee","48": "Texas","49": "Utah","50": "Vermont","51": "Virginia",
            "53": "Washington","54": "West Virginia","55": "Wisconsin","56": "Wyoming"
        }
        gdf["STATE_NAME"] = gdf["STATEFP"].map(state_map)

    gdf["region_id"] = gdf["STATEFP"] + gdf["COUNTYFP"]
    gdf["region_name"] = gdf["NAME"] + ", " + gdf["STATE_NAME"]

    out = gdf[["region_id", "region_name", "geometry"]].copy()
    out.to_file(REGIONS_DIR / "us_counties_contiguous.geojson", driver="GeoJSON")
    print("Saved us_counties_contiguous.geojson")


def build_metros_all():
    zip_path = download(CBSA_URL, TMP_DIR / Path(CBSA_URL).name)
    gdf = read_zipped_shapefile(zip_path).to_crs(4326)

    # Keep all CBSAs. If you later want only metro (not micro), filter by LSAD.
    gdf["region_id"] = gdf["GEOID"]
    gdf["region_name"] = gdf["NAME"]

    out = gdf[["region_id", "region_name", "geometry"]].copy()
    out.to_file(REGIONS_DIR / "us_metros_all.geojson", driver="GeoJSON")
    print("Saved us_metros_all.geojson")


def build_cities_top200():
    zip_path = download(PLACE_URL, TMP_DIR / Path(PLACE_URL).name)
    gdf = read_zipped_shapefile(zip_path).to_crs(4326)

    gdf = gdf[~gdf["STATEFP"].isin(STATE_EXCLUDE)].copy()

    # ACS 5-year place population
    pop_url = (
        f"https://api.census.gov/data/{ACS_YEAR}/acs/acs5"
        f"?get=NAME,B01003_001E&for=place:*&in=state:*"
    )
    pop = census_json(pop_url)
    pop["region_id"] = pop["state"] + pop["place"]
    pop["population"] = pd.to_numeric(pop["B01003_001E"], errors="coerce")

    gdf["region_id"] = gdf["STATEFP"] + gdf["PLACEFP"]
    merged = gdf.merge(pop[["region_id", "population"]], on="region_id", how="left")

    merged = merged.sort_values("population", ascending=False).head(CITIES_TOP_N).copy()
    merged["region_name"] = merged["NAME"]

    out = merged[["region_id", "region_name", "geometry"]].copy()
    out.to_file(REGIONS_DIR / f"us_cities_top{CITIES_TOP_N}.geojson", driver="GeoJSON")
    print(f"Saved us_cities_top{CITIES_TOP_N}.geojson")


def main():
    build_states()
    build_counties()
    build_metros_all()
    build_cities_top200()


if __name__ == "__main__":
    main()