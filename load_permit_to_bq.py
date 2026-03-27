# import libraries
import google.auth
import pandas as pd
import pandas_gbq
import requests

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

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$select": select_cols,
            "$order": "issued_date DESC",
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


def upload_to_bq(df_permits: pd.DataFrame) -> None:
    """Upload df to BigQuery using the `TRUNCATE` technique.
    use `if_exists = "replace"`
    Method: drop existinf table, recreates it and insets current data"""
    pandas_gbq.to_gbq(
        df_permits,
        table_id,
        project_id=project_id,
        if_exists="replace",
    )
    print(f"Uploaded {len(df_permits):,} rows to {table_id}")


# Main
if __name__ == "__main__":
    print("Fetching permits data from NYC Open Data...")
    df_permits = fetch_permits()
    print(f"Total rows fetched: {len(df_permits):,}")
    upload_to_bq(df_permits)
    print("Done! Check BigQuery console to verify the table")
