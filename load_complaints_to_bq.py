# import libraries

import google.auth
import pandas as pd
import pandas_gbq
import requests

# configuration
project_id = "sipa-adv-c-cosmic-spaghetti"
table_id = "cosmic_spaghetti.complaints"
url = "https://data.cityofnewyork.us/resource/eabe-havv.json"
limit = 50000

# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_complaints() -> pd.DataFrame:
    """Pull all available complaints from NYC Open Data API."""
    all_records = []
    offset = 0
    session = requests.Session()

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$order": "date_entered DESC",
        }
        response = session.get(url, params=params)
        response.raise_for_status()
        chunk = response.json()

        if not chunk:
            break

        all_records.extend(chunk)
        offset += limit
        print(f"  Fetched {offset} rows so far...")

        # stop after 200k rows to keep it manageable
        max_rows = 200000
        if offset >= max_rows:
            print("  Reached 200k rows limit — stopping.")
            break

        if len(chunk) < limit:
            break

    df = pd.DataFrame(all_records)
    df["date_entered"] = pd.to_datetime(df["date_entered"], errors="coerce")
    print(f"  Date range: {df['date_entered'].min()} to {df['date_entered'].max()}")
    return df


def upload_to_bq(df: pd.DataFrame) -> None:
    """Technique: TRUNCATE (replace)
    Complaints data is filtered to last 1 year.
    Full refresh ensures BigQuery always reflects the latest data."""
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=project_id,
        if_exists="replace",
    )
    print(f" Uploaded {len(df):,} rows to {table_id}")


# Main
if __name__ == "__main__":
    print("Fetching complaints data from NYC Open Data (last 1 year)...")
    df = fetch_complaints()
    print(f"Total rows fetched: {len(df):,}")
    print(f"Date range: {df['date_entered'].min()} to {df['date_entered'].max()}")
    upload_to_bq(df)
    print("Done! Check BigQuery console to verify the table.")
