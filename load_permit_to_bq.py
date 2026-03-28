# import libraries
from datetime import datetime, timedelta

import google.auth
import pandas as pd
import pandas_gbq
import requests
from google.cloud import bigquery

# configuration
project_id = "sipa-adv-c-cosmic-spaghetti"
table_id = "cosmic_spaghetti.permits"

url = "https://data.cityofnewyork.us/resource/rbx6-tga4.json"
limit = 50000
select_cols = (
    "borough,issued_date,approved_date,expired_date,work_type,permit_status,community_board"
)

# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_permits() -> pd.DataFrame:
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
        print(f" Fetched {offset} rows so far...")
        if len(chunk) < limit:
            break

    df_permits = pd.DataFrame(all_records)
    df_permits["issued_date"] = pd.to_datetime(df_permits["issued_date"], errors="coerce")
    return df_permits


def get_existing_keys() -> set:
    """get existing issueddates in BQ"""
    client = bigquery.Client(credentials=credentials, project=project_id)
    try:
        query = f"SELECT DISTINCT CAST(issued_date AS STRING) FROM `{project_id}.{table_id}`"
        existing = client.query(query).to_dataframe()
        return set(existing.iloc[:, 0].tolist())
    except Exception:
        return set()


def upload_to_bq(df_permits: pd.DataFrame) -> None:
    """Upload df to BigQuery using the `TRUNCATE` technique.
    use `if_exists = "replace"`
    Method: drop existinf table, recreates it and insets current data"""

    existing_keys = get_existing_keys()

    if existing_keys:
        # filter to only new rows
        df_permits["issued_date_str"] = df_permits["issued_date"].astype(str)
        df_new = df_permits[~df_permits["issued_date_str"].isin(existing_keys)]
        df_new = df_new.drop(columns=["issued_date_str"])
        print(f"  Found {len(existing_keys):,} existing rows in BigQuery")
        print(f"  New rows to insert: {len(df_new):,}")
    else:
        # first run — insert everything
        df_new = df_permits
        print(f"  First run — inserting all {len(df_new):,} rows")

    if df_new.empty:
        print("No new rows to insert, table is already up to date.")
        return

    pandas_gbq.to_gbq(
        df_new,
        table_id,
        project_id=project_id,
        if_exists="append",
    )
    print(f"Uploaded {len(df_new):,} rows to {table_id}")


# Main
if __name__ == "__main__":
    print("Fetching permits data from NYC Open Data...")
    df_permits = fetch_permits()
    print(f"Total rows fetched: {len(df_permits):,}")
    upload_to_bq(df_permits)
    print("Done! Check BigQuery console to verify the table")
