# import libraries
import google.auth
import pandas as pd
import pandas_gbq
import requests

# configuration
project_id = "sipa-adv-c-cosmic-spaghetti"
table_id = "cosmic_spaghetti.permits"

# Dataset 1 — DOB NOW approved permits (newer filings, work permits: GC, PL, ME)
URL_NOW = "https://data.cityofnewyork.us/resource/rbx6-tga4.json"

# Dataset 2 — DOB Permit Issuance (older system, job types: NB, DM, A1, A2, A3)
# Also has LATITUDE / LONGITUDE
URL_ISSUANCE = "https://data.cityofnewyork.us/resource/ipu4-2q9a.json"

limit = 50000


# borough code to name mapping for ipu4-2q9a
BOROUGH_MAP = {
    "1": "MANHATTAN",
    "2": "BRONX",
    "3": "BROOKLYN",
    "4": "QUEENS",
    "5": "STATEN ISLAND",
}

# permit type descriptions
JOB_TYPE_MAP = {
    "NB": "New Building",
    "DM": "Demolition",
    "A1": "Alteration Type 1",
    "A2": "Alteration Type 2",
    "A3": "Alteration Type 3",
    "GC": "General Construction",
    "PL": "Plumbing",
    "ME": "Mechanical/Electrical",
    "SG": "Sign",
    "EQ": "Equipment",
    "BL": "Boiler",
    "EW": "Earthwork",
    "FA": "Fire Alarm",
    "FB": "Fuel Burning",
    "FP": "Fire Suppression",
    "FS": "Fuel Storage",
    "OT": "Other",
}

START_DATE = "2025-01-01T00:00:00"

# local authentication
credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/bigquery"])
pandas_gbq.context.credentials = credentials
pandas_gbq.context.project = project_id


def fetch_paginated(url: str, params: dict) -> list:
    """Generic paginated fetch from NYC Open Data API."""
    all_records = []
    offset = 0
    session = requests.Session()

    while True:
        params["$limit"] = limit
        params["$offset"] = offset
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

    return all_records


def fetch_now_permits() -> pd.DataFrame:
    print("\nFetching DOB NOW permits (rbx6-tga4) from Jan 2025...")
    params = {
        "$where": f"issued_date >= '{START_DATE}'",  # use constant
    }
    records = fetch_paginated(URL_NOW, params)
    df = pd.DataFrame(records)

    if df.empty:
        return pd.DataFrame()

    df = df.rename(
        columns={
            "issued_date": "permit_date",
            "work_type": "permit_type",
            "permit_status": "status",
        }
    )
    df["latitude"] = pd.to_numeric(df.get("latitude"), errors="coerce")
    df["longitude"] = pd.to_numeric(df.get("longitude"), errors="coerce")
    df["source"] = "DOB_NOW"
    return df


def fetch_issuance_permits() -> pd.DataFrame:
    print("\nFetching DOB Permit Issuance (ipu4-2q9a) — NB jobs only...")
    params = {
        "$where": "job_type = 'NB'",  # filter by job type only
    }
    records = fetch_paginated(URL_ISSUANCE, params)
    df = pd.DataFrame(records)

    if df.empty:
        return pd.DataFrame()

    keep = [
        "borough",
        "job_type",
        "permit_status",
        "community_board",
        "gis_latitude",
        "gis_longitude",
        "filing_date",
    ]
    df = df[[c for c in keep if c in df.columns]].copy()

    df = df.rename(
        columns={
            "job_type": "permit_type",
            "permit_status": "status",
            "gis_latitude": "latitude",
            "gis_longitude": "longitude",
            "filing_date": "permit_date",
        }
    )
    df["borough"] = df["borough"].str.strip().str.upper()
    df["source"] = "DOB_ISSUANCE"
    return df


def upload_to_bq(df: pd.DataFrame) -> None:
    """Technique: TRUNCATE (replace)
    Combined permits from DOB NOW and DOB Permit Issuance.
    Starting from January 2025.
    Full refresh on each run."""
    pandas_gbq.to_gbq(
        df,
        table_id,
        project_id=project_id,
        if_exists="replace",
    )
    print(f"\n Uploaded {len(df):,} rows to {table_id}")


# Main
if __name__ == "__main__":
    # fetch both datasets
    df_now = fetch_now_permits()
    df_issuance = fetch_issuance_permits()

    # combine
    df_combined = pd.concat([df_now, df_issuance], ignore_index=True)

    # standardize
    df_combined["permit_date"] = pd.to_datetime(
        df_combined["permit_date"], format="mixed", errors="coerce"
    )
    df_combined["borough"] = df_combined["borough"].str.strip().str.upper()
    df_combined["permit_type"] = df_combined["permit_type"].str.strip().str.upper()
    df_combined["latitude"] = pd.to_numeric(df_combined["latitude"], errors="coerce")
    df_combined["longitude"] = pd.to_numeric(df_combined["longitude"], errors="coerce")

    # add human readable description
    df_combined["permit_type_desc"] = (
        df_combined["permit_type"].map(JOB_TYPE_MAP).fillna(df_combined["permit_type"])
    )

    print(f"\nTotal rows combined: {len(df_combined):,}")
    print(f"  DOB NOW rows:       {len(df_now):,}")
    print(f"  DOB Issuance rows:  {len(df_issuance):,}")
    print("\nPermit types breakdown:")
    print(df_combined["permit_type"].value_counts().to_string())
    print(f"\nRows with lat/lon: {df_combined['latitude'].notna().sum():,}")

    upload_to_bq(df_combined)
    print("Done! Check BigQuery console to verify the table.")
