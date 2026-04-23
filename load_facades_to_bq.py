# import libraries
import google.auth
import pandas as pd
import pandas_gbq
import requests

# configuration
project_id = "sipa-adv-c-cosmic-spaghetti"
table_id = "cosmic_spaghetti.facades"

# DOB NOW: Safety — Facades Compliance Filings (FISP / Local Law 11)
# Buildings taller than 6 stories must be inspected every 5 years
URL = "https://data.cityofnewyork.us/resource/xubg-57si.json"

limit = 50000

# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_facades() -> pd.DataFrame:
    """Pull FISP facade compliance filings from NYC Open Data API."""
    all_records = []
    offset = 0
    session = requests.Session()

    print("Fetching FISP facade compliance filings...")
    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
        }
        response = session.get(URL, params=params)
        response.raise_for_status()
        chunk = response.json()

        if not chunk:
            break
        all_records.extend(chunk)
        offset += limit
        print(f"  Fetched {offset} rows so far...")
        if len(chunk) < limit:
            break

    df = pd.DataFrame(all_records)
    if df.empty:
        return df

    df["filing_date"] = pd.to_datetime(df["filing_date"], errors="coerce")
    df["borough"] = df["borough"].str.strip().str.upper()
    df["filing_status"] = df["filing_status"].str.strip().str.upper()
    df["current_status"] = df["current_status"].str.strip().str.upper()
    return df


def upload_to_bq(df: pd.DataFrame) -> None:
    """Technique: TRUNCATE (replace)
    FISP facade compliance filings — full refresh on each run.
    Dataset is updated as new filings come in each 5-year cycle."""
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=project_id,
        if_exists="replace",
    )
    print(f" Uploaded {len(df):,} rows to {table_id}")


# Main
if __name__ == "__main__":
    df = fetch_facades()
    print(f"\nTotal filings fetched: {len(df):,}")
    if not df.empty:
        print(f"\nFiling status breakdown:")
        print(df["filing_status"].value_counts().to_string())
        print(f"\nBorough breakdown:")
        print(df["borough"].value_counts().to_string())
    upload_to_bq(df)
    print("Done! Check BigQuery console to verify the table.")
