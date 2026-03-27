from pathlib import Path
import json
import re
import requests
import pandas as pd

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

STATE_NAME_TO_ABBR = {
    "alabama": "AL",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}

STATE_FIPS_TO_ABBR = {
    "01": "AL",
    "04": "AZ",
    "05": "AR",
    "06": "CA",
    "08": "CO",
    "09": "CT",
    "10": "DE",
    "11": "DC",
    "12": "FL",
    "13": "GA",
    "16": "ID",
    "17": "IL",
    "18": "IN",
    "19": "IA",
    "20": "KS",
    "21": "KY",
    "22": "LA",
    "23": "ME",
    "24": "MD",
    "25": "MA",
    "26": "MI",
    "27": "MN",
    "28": "MS",
    "29": "MO",
    "30": "MT",
    "31": "NE",
    "32": "NV",
    "33": "NH",
    "34": "NJ",
    "35": "NM",
    "36": "NY",
    "37": "NC",
    "38": "ND",
    "39": "OH",
    "40": "OK",
    "41": "OR",
    "42": "PA",
    "44": "RI",
    "45": "SC",
    "46": "SD",
    "47": "TN",
    "48": "TX",
    "49": "UT",
    "50": "VT",
    "51": "VA",
    "53": "WA",
    "54": "WV",
    "55": "WI",
    "56": "WY",
}


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
    t = t.replace("st.", "saint")
    t = t.replace("ste.", "sainte")
    t = t.replace("metropolitan statistical area", "")
    t = t.replace("micropolitan statistical area", "")
    t = t.replace("metropolitan division", "")
    t = t.replace(" city and borough", "")
    t = t.replace("borough", "")
    t = t.replace(" census area", "")
    t = t.replace(" municipality", "")
    t = t.replace(" charter township", "")
    t = t.replace(" township", "")
    t = t.replace(" village", "")
    t = t.replace(" town", "")
    t = t.replace(" city", "")
    t = t.replace(" county", "")
    t = t.replace(" parish", "")
    t = re.sub(r"[^a-z0-9]+", " ", t)
    return re.sub(r"\s+", " ", t).strip()


def split_city_and_state(area_text: str) -> tuple[str, str | None]:
    txt = str(area_text)
    if "," in txt:
        city, state = txt.rsplit(",", 1)
        state = state.strip().lower()
        return normalize(city), STATE_NAME_TO_ABBR.get(state, state.upper())
    return normalize(txt), None


def load_region_names() -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}

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
            row = {
                "region_id": str(props["region_id"]),
                "region_name": str(props["region_name"]),
            }

            if dataset == "cities":
                rid = str(props["region_id"]).zfill(7)
                state_fips = rid[:2]
                row["state_abbr"] = STATE_FIPS_TO_ABBR.get(state_fips)

            rows.append(row)

        rdf = pd.DataFrame(rows)
        rdf["region_name_norm"] = rdf["region_name"].map(normalize)
        out[dataset] = rdf

    return out


def parse_bls_area_keys(area_code: str) -> dict[str, str]:
    area_code = str(area_code)

    if area_code.startswith("ST") and len(area_code) >= 4:
        return {"state_fips": area_code[2:4]}

    if area_code.startswith("CN") and len(area_code) >= 7:
        return {"county_fips": area_code[2:7]}

    return {}


def build_dataset(dataset_name: str, data_file_key: str) -> None:
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

    elif dataset_name == "cities":
        data[["city_name_norm", "state_abbr"]] = data["area_text"].apply(
            lambda x: pd.Series(split_city_and_state(x))
        )

        regions["city_name_norm"] = regions["region_name"].map(normalize)

        merged = data.merge(
            regions[["region_id", "region_name", "city_name_norm", "state_abbr"]],
            on=["city_name_norm", "state_abbr"],
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

    if merged.empty:
        print(f"{dataset_name}: no matched rows found")
        out_path = DATA_DIR / f"{dataset_name}_laus_monthly.csv"
        pd.DataFrame(columns=[
            "date",
            "region_id",
            "region_name",
            "bls_unemployment_rate",
            "bls_unemployment",
            "bls_employment",
            "bls_labor_force",
            "dataset_name",
        ]).to_csv(out_path, index=False)
        print(f"Saved empty {out_path}")
        return

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

    for col in rename.values():
        if col not in pivot.columns:
            pivot[col] = pd.NA

    pivot["dataset_name"] = dataset_name
    pivot = pivot.sort_values(["region_id", "date"]).reset_index(drop=True)

    out_path = DATA_DIR / f"{dataset_name}_laus_monthly.csv"
    pivot.to_csv(out_path, index=False)
    print(f"Saved {out_path} with {len(pivot)} rows.")


def check_required_files() -> None:
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


def main() -> None:
    check_required_files()
    build_dataset("states", "states_u")
    build_dataset("metros", "metro")
    build_dataset("counties", "county")
    build_dataset("cities", "city")


if __name__ == "__main__":
    main()