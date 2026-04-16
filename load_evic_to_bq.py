# import libraries
import google.auth
import pandas as pd
import pandas_gbq
import requests

# configuration
project_id = "sipa-adv-c-cosmic-spaghetti"
table_id = "cosmic_spaghetti.evictions"

url = "https://data.cityofnewyork.us/resource/6z8x-wfk4.json"
limit = 50000
select_cols = "executed_date,borough,residential_commercial_ind"

# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_evic() -> pd.DataFrame:
    all_records = []
    offset = 0
    session = requests.Session()

    while True:
        params = {
            "$limit": limit,
            "$offset": offset,
            "$select": select_cols,
            "$order": "executed_date DESC",
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

    df_evic = pd.DataFrame(all_records)
    df_evic["executed_date"] = pd.to_datetime(df_evic["executed_date"], errors="coerce")
    return df_evic


def upload_to_bq(df_evic: pd.DataFrame) -> None:
    """Upload df to BigQuery using the `TRUNCATE` technique.
    use `if_exists = "replace"`
    Method: drop existing table, recreates it and insets current data"""
    pandas_gbq.to_gbq(
        df_evic,
        table_id,
        project_id=project_id,
        if_exists="replace",
    )
    print(f"Uploaded {len(df_evic):,} rows to {table_id}")


# Main
if __name__ == "__main__":
    print("Fetching evictions data from NYC Open Data...")
    df_evic = fetch_evic()
    print(f"Total rows fetched: {len(df_evic):,}")
    upload_to_bq(df_evic)
    print("Done! Check BigQuery console to verify the table")
