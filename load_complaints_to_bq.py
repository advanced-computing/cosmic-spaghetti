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
MAX_ROWS = 500_000
MIN_YEAR = 2015


# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_complaints() -> pd.DataFrame:
    """Pull complaints from 2015 onwards from NYC Open Data API.
    Fetches ordered by most recent first, stops when records go before 2015."""
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

        # stop early if oldest record in chunk is before 2015
        chunk_df = pd.DataFrame(chunk)
        if "date_entered" in chunk_df.columns:
            oldest = pd.to_datetime(chunk_df["date_entered"].iloc[-1], errors="coerce")
            if oldest is not pd.NaT and oldest.year < MIN_YEAR:
                print(f"  Reached records before {MIN_YEAR} — stopping early.")
                break

        # safety cap
        if offset >= MAX_ROWS:
            print(f"  Reached {MAX_ROWS:,} row limit — stopping.")
            break

        if len(chunk) < limit:
            break

    df = pd.DataFrame(all_records)
    df["date_entered"] = pd.to_datetime(df["date_entered"], errors="coerce")

    # filter to 2015 onwards in Python to catch any stray older records
    df = df[df["date_entered"].dt.year >= MIN_YEAR]
    print(f"  After filtering to {MIN_YEAR}+: {len(df):,} rows")
    print(f"  Date range: {df['date_entered'].min()} to {df['date_entered'].max()}")
    return df


def upload_to_bq(df: pd.DataFrame) -> None:
    """Technique: TRUNCATE (replace)
    Complaints data filtered to 2015 onwards.
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
    print(f"Fetching complaints data from NYC Open Data ({MIN_YEAR} onwards)...")
    df = fetch_complaints()
    print(f"Total rows loaded: {len(df):,}")
    upload_to_bq(df)
    print("Done! Check BigQuery console to verify the table.")
