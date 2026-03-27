from pathlib import Path
import re
import requests
import pandas as pd
import json

DATA_DIR = Path("data/bls")
DATA_DIR.mkdir(parents=True, exist_ok=True)

BASE = "https://download.bls.gov/pub/time.series/la"
FILES = {
    "area": "la.area",
    "series": "la.series",
    "measure": "la.measure",
    "states_u": "la.data.2.AllStatesU",
    "metro": "la.data.60.Metro",
    "county": "la.data.64.County",
    "city": "la.data.65.City",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "*/*",
}

SESSION = requests.Session()
SESSION.headers.update(HEADERS)


def resolve_local_file(name: str) -> Path | None:
    candidates = [
        DATA_DIR / name,
        DATA_DIR / f"{name}.txt",
    ]
    for path in candidates:
        if path.exists() and path.stat().st_size > 0:
            return path
    return None


def download(name: str) -> Path:
    local = resolve_local_file(name)
    if local is not None:
        print(f"Using local file: {local}")
        return local

    url = f"{BASE}/{name}"
    print(f"Downloading: {url}")
    r = SESSION.get(url, timeout=300)
    r.raise_for_status()

    out_path = DATA_DIR / f"{name}.txt"
    out_path.write_bytes(r.content)
    return out_path


def read_bls_tsv(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, sep="\t", dtype=str, low_memory=False)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def normalize(text: str) -> str:
    if text is None:
        return ""
    t = str(text).lower().strip()
    t = t.replace("&", "and")
    t = t.replace("metropolitan statistical area", "")
    t = t.replace("micropolitan statistical area", "")
    t = t.replace(" metropolitan division", "")
    t = t.replace(" county", "")
    t = t.replace(" parish", "")
    t = t.replace(" census area", "")
    t = t.replace(" city and borough", "")
    t = t.replace(" borough", "")
    t = t.replace(" city", "")
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()



def load_region_names():
    out = {}

    region_files = {
        "states": Path("data/regions/us_states_contiguous.geojson"),
        "metros": Path("data/regions/us_metros_all.geojson"),
        "counties": Path("data/regions/us_counties_contiguous.geojson"),
        "cities": Path("data/regions/us_cities_top200.geojson"),
    }

    for dataset, path in region_files.items():
        with open(path, "r", encoding="utf-8") as f:
            geojson = json.load(f)

        rows = []
        for feature in geojson["features"]:
            props = feature["properties"]
            rows.append(
                {
                    "region_id": str(props["region_id"]),
                    "region_name": str(props["region_name"]),
                }
            )

        rdf = pd.DataFrame(rows)
        rdf["region_name_norm"] = rdf["region_name"].map(normalize)
        out[dataset] = rdf

    return out

def parse_bls_area_keys(area_code: str):
    area_code = str(area_code)

    if area_code.startswith("ST") and len(area_code) >= 4:
        return {"state_fips": area_code[2:4]}

    if area_code.startswith("CN") and len(area_code) >= 7:
        # BLS county codes look like CN + 5-digit fips + padding
        return {"county_fips": area_code[2:7]}

    return {}


def build_dataset(dataset_name: str, data_file_key: str):
    area = read_bls_tsv(download(FILES["area"]))
    series = read_bls_tsv(download(FILES["series"]))
    data = read_bls_tsv(download(FILES[data_file_key]))

    series = series[series["measure_code"].isin(["03", "04", "05", "06"])].copy()
    series = series[series["seasonal"] == "U"].copy()

    area_small = area[["area_code", "area_text"]].drop_duplicates()
    series = series.merge(area_small, on="area_code", how="left")
    data = data.merge(
        series[["series_id", "area_code", "area_text", "measure_code"]],
        on="series_id",
        how="inner",
    )

    data["year"] = pd.to_numeric(data["year"], errors="coerce")
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    data = data[data["period"].str.startswith("M", na=False)].copy()
    data["month"] = pd.to_numeric(data["period"].str[1:], errors="coerce")
    data["date"] = pd.to_datetime(
        dict(year=data["year"], month=data["month"], day=1),
        errors="coerce",
    )

    data["state_fips"] = None
    data["county_fips"] = None

    parsed = data["area_code"].map(parse_bls_area_keys)
    data["state_fips"] = parsed.map(lambda x: x.get("state_fips"))
    data["county_fips"] = parsed.map(lambda x: x.get("county_fips"))

    regions = load_region_names()[dataset_name].copy()

    if dataset_name == "states":
        regions["state_fips"] = regions["region_id"].str.zfill(2)
        merged = data.merge(
            regions[["region_id", "region_name", "state_fips"]],
            on="state_fips",
            how="inner",
        )

    elif dataset_name == "counties":
        regions["county_fips"] = regions["region_id"].str.zfill(5)
        merged = data.merge(
            regions[["region_id", "region_name", "county_fips"]],
            on="county_fips",
            how="inner",
        )

    else:
        data["area_text_norm"] = data["area_text"].map(normalize)
        merged = data.merge(
            regions[["region_id", "region_name", "region_name_norm"]],
            left_on="area_text_norm",
            right_on="region_name_norm",
            how="inner",
        )

    pivot = (
        merged.pivot_table(
            index=["date", "region_id", "region_name"],
            columns="measure_code",
            values="value",
            aggfunc="first",
        )
        .reset_index()
        .rename_axis(None, axis=1)
    )

    rename = {
        "03": "bls_unemployment_rate",
        "04": "bls_unemployment",
        "05": "bls_employment",
        "06": "bls_labor_force",
    }
    pivot = pivot.rename(columns=rename)
    pivot["dataset_name"] = dataset_name
    pivot = pivot.sort_values(["region_id", "date"]).reset_index(drop=True)

    out_path = DATA_DIR / f"{dataset_name}_laus_monthly.csv"
    pivot.to_csv(out_path, index=False)
    print(f"Saved {out_path} with {len(pivot)} rows.")


def check_required_files():
    required = [
        FILES["area"],
        FILES["series"],
        FILES["measure"],
        FILES["states_u"],
        FILES["metro"],
        FILES["county"],
        FILES["city"],
    ]

    missing = []
    for name in required:
        if resolve_local_file(name) is None:
            missing.append(name)

    if missing:
        print("Missing local BLS files:")
        for name in missing:
            print(f"  - {name} or {name}.txt")
    else:
        print("All required local BLS files found.")


def main():
    check_required_files()
    build_dataset("states", "states_u")
    build_dataset("metros", "metro")
    build_dataset("counties", "county")
    build_dataset("cities", "city")


if __name__ == "__main__":
    main()