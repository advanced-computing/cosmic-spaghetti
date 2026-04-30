# import libraries
import io

import google.auth
import pandas as pd
import pandas_gbq
import requests

# configuration
project_id = "sipa-adv-c-cosmic-spaghetti"
table_summary = "cosmic_spaghetti.buildings_summary"

url = "https://data.cityofnewyork.us/resource/5zhs-2jue.json"
limit = 50000

BOROUGH_MAP = {
    "1": "MANHATTAN",
    "2": "BRONX",
    "3": "BROOKLYN",
    "4": "QUEENS",
    "5": "STATEN ISLAND",
}

# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_buildings() -> list:
    """Fetch building footprints via CSV endpoint — avoids geometry API issues."""
    all_records = []
    offset = 0
    session = requests.Session()

    # use CSV endpoint instead of JSON — works on geospatial datasets
    csv_url = "https://data.cityofnewyork.us/resource/5zhs-2jue.csv"

    print("Fetching NYC building footprints (CSV endpoint)...")
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
        }
        response = session.get(csv_url, params=params)
        response.raise_for_status()

        df_chunk = pd.read_csv(io.StringIO(response.text))
        if df_chunk.empty:
            break

        all_records.extend(df_chunk.to_dict("records"))
        offset += limit
        print(f"  Fetched {offset} rows so far...")
        if len(df_chunk) < limit:
            break

    return all_records


def upload_to_bq(df: pd.DataFrame, table_id: str) -> None:
    """Upload dataframe to BigQuery using truncate."""
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=project_id,
        if_exists="replace",
    )
    print(f" Uploaded {len(df):,} rows to {table_id}")


def process_buildings(records: list) -> pd.DataFrame:
    df = pd.DataFrame(records)

    # extract borough from first digit of bin
    df["boro_code"] = df["bin"].astype(str).str[0]
    df["borough"] = df["boro_code"].map(BOROUGH_MAP).fillna("UNKNOWN")

    # correct column names from CSV
    df["cnstrct_yr"] = pd.to_numeric(df["construction_year"], errors="coerce")
    df["heightroof"] = pd.to_numeric(df["height_roof"], errors="coerce")
    df["shape_area"] = pd.to_numeric(df["shape_area"], errors="coerce")

    df_summary = (
        df.groupby(["borough", "cnstrct_yr"])
        .agg(
            total_buildings=("borough", "count"),
            avg_height=("heightroof", "mean"),
            avg_area=("shape_area", "mean"),
        )
        .reset_index()
    )
    return df_summary


# Main
if __name__ == "__main__":
    print("Step 1: Fetching building footprints...")
    records = fetch_buildings()
    print(f"Total records fetched: {len(records):,}")

    print("\nStep 2: Aggregating...")
    df_summary = process_buildings(records)

    print("\nBorough summary:")
    print(
        df_summary.groupby("borough")["total_buildings"]
        .sum()
        .sort_values(ascending=False)
        .to_string()
    )

    print("\nStep 3: Uploading to BigQuery...")
    upload_to_bq(df_summary, table_summary)
    print(f"\nDone! Table created: {table_summary}")
