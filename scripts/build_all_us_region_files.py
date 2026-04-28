from pathlib import Path
import shutil
import requests
import geopandas as gpd

TMP = Path("data/tmp")
REGIONS = Path("data/regions")
DOCS_REGIONS = Path("docs/data/regions")

TMP.mkdir(parents=True, exist_ok=True)
REGIONS.mkdir(parents=True, exist_ok=True)
DOCS_REGIONS.mkdir(parents=True, exist_ok=True)

STATE_URL = "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_state_500k.zip"
COUNTY_URL = "https://www2.census.gov/geo/tiger/GENZ2024/shp/cb_2024_us_county_500k.zip"

# Exclude only territories. Keep Alaska, Hawaii, and DC.
EXCLUDE_STATEFP = {"60", "66", "69", "72", "78"}

def download(url):
    out = TMP / url.split("/")[-1]
    if out.exists() and out.stat().st_size > 0:
        return out

    print(f"Downloading {url}")
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    out.write_bytes(r.content)
    return out

def read_zip(path):
    return gpd.read_file(f"zip://{path.resolve()}").to_crs(4326)

def save(gdf, filename):
    local = REGIONS / filename
    docs = DOCS_REGIONS / filename

    gdf.to_file(local, driver="GeoJSON")
    shutil.copyfile(local, docs)

    print(f"Saved {local}")
    print(f"Copied {docs}")

def build_states():
    zip_path = download(STATE_URL)
    gdf = read_zip(zip_path)

    gdf["STATEFP"] = gdf["STATEFP"].astype(str).str.zfill(2)
    gdf = gdf[~gdf["STATEFP"].isin(EXCLUDE_STATEFP)].copy()

    out = gdf[["STATEFP", "NAME", "geometry"]].copy()
    out = out.rename(columns={"STATEFP": "region_id", "NAME": "region_name"})
    out["region_id"] = out["region_id"].astype(str).str.zfill(2)

    save(out, "us_states_all.geojson")

def build_counties():
    zip_path = download(COUNTY_URL)
    gdf = read_zip(zip_path)

    gdf["STATEFP"] = gdf["STATEFP"].astype(str).str.zfill(2)
    gdf["COUNTYFP"] = gdf["COUNTYFP"].astype(str).str.zfill(3)
    gdf = gdf[~gdf["STATEFP"].isin(EXCLUDE_STATEFP)].copy()

    state_names = {
        "01": "Alabama", "02": "Alaska", "04": "Arizona", "05": "Arkansas",
        "06": "California", "08": "Colorado", "09": "Connecticut",
        "10": "Delaware", "11": "District of Columbia", "12": "Florida",
        "13": "Georgia", "15": "Hawaii", "16": "Idaho", "17": "Illinois",
        "18": "Indiana", "19": "Iowa", "20": "Kansas", "21": "Kentucky",
        "22": "Louisiana", "23": "Maine", "24": "Maryland",
        "25": "Massachusetts", "26": "Michigan", "27": "Minnesota",
        "28": "Mississippi", "29": "Missouri", "30": "Montana",
        "31": "Nebraska", "32": "Nevada", "33": "New Hampshire",
        "34": "New Jersey", "35": "New Mexico", "36": "New York",
        "37": "North Carolina", "38": "North Dakota", "39": "Ohio",
        "40": "Oklahoma", "41": "Oregon", "42": "Pennsylvania",
        "44": "Rhode Island", "45": "South Carolina", "46": "South Dakota",
        "47": "Tennessee", "48": "Texas", "49": "Utah", "50": "Vermont",
        "51": "Virginia", "53": "Washington", "54": "West Virginia",
        "55": "Wisconsin", "56": "Wyoming",
    }

    gdf["region_id"] = gdf["STATEFP"] + gdf["COUNTYFP"]
    gdf["state_name"] = gdf["STATEFP"].map(state_names)
    gdf["region_name"] = gdf["NAME"] + ", " + gdf["state_name"]

    out = gdf[["region_id", "region_name", "geometry"]].copy()
    save(out, "us_counties_all.geojson")

def main():
    build_states()
    build_counties()

if __name__ == "__main__":
    main()