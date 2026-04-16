# import libraries
from datetime import datetime, timedelta

import google.auth
import pandas as pd
import pandas_gbq
import requests

# configuration
project_id = "sipa-adv-c-cosmic-spaghetti"
table_id = "cosmic_spaghetti.permits"

url = "https://data.cityofnewyork.us/resource/rbx6-tga4.json"
limit = 50000
select_cols = "borough,issued_date,work_type,permit_status,community_board"

# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_permits() -> pd.DataFrame:
    """Pull permits from last 1 year from NYC Open Data API with pagination."""
    all_records = []
    offset = 0
    session = requests.Session()

    # filter to last 1 year
    one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%dT%H:%M:%S")
    today = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$select": select_cols,
            "$order": "issued_date DESC",
            "$where": f"issued_date >= '{one_year_ago}' AND issued_date <= '{today}'",
        }
        response = session.get(url, params=params)
        response.raise_for_status()
        chunk = response.json()

        if not chunk:
            break
        all_records.extend(chunk)
        offset += limit
        print(f"  Fetched {offset} rows so far...")
        if len(chunk) < limit:
            break

    df_permits = pd.DataFrame(all_records)
    df_permits["issued_date"] = pd.to_datetime(df_permits["issued_date"], errors="coerce")
    return df_permits


def upload_to_bq(df_permits: pd.DataFrame) -> None:
    """Method: TRUNCATE (replace)
    Permits data is filtered to last 1 year from the API.
    Full refresh ensures BigQuery always reflects the latest data.
    if_exists='replace' drops and recreates the table on each run.
    Chosen over incremental because:
    - Dataset is already filtered to 1 year so size is manageable
    - Permit records can be updated/corrected over time
    - No reliable unique key available without billing (no DML allowed)
    """
    pandas_gbq.to_gbq(
        df_permits,
        table_id,
        project_id=project_id,
        if_exists="replace",
    )
    print(f" Uploaded {len(df_permits):,} rows to {table_id}")


# Main
if __name__ == "__main__":
    print("Fetching permits data from NYC Open Data (last 1 year)...")
    df_permits = fetch_permits()
    print(f"Total rows fetched: {len(df_permits):,}")
    upload_to_bq(df_permits)
    print("Done! Check BigQuery console to verify the table.")
