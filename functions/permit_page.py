from __future__ import annotations

import re
from typing import Optional

import pandas as pd
import requests

borough_code = {
    "1": "Manhattan",
    "2": "Bronx",
    "3": "Brooklyn",
    "4": "Queens",
    "5": "Staten Island",
    1: "Manhattan",
    2: "Bronx",
    3: "Brooklyn",
    4: "Queens",
    5: "Staten Island",
}


# return first column name from dataset (anticipating changing in dataset)
def first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# discover desired coliums
def discover_columns(url: str, timeout: int = 30) -> set[str]:

    m = re.search(r"/resource/([a-z0-9]{4}-[a-z0-9]{4})\.json", url)
    if not m:
        return set()

    dataset_id = m.group(1)
    meta_url = f"https://data.cityofnewyork.us/api/views/{dataset_id}.json"
    disc = requests.get(meta_url, timeout=timeout)
    disc.raise_for_status()
    meta = disc.json()

    cols = set()
    for c in meta.get("columns", []):
        field = c.get("fieldName")
        if field:
            cols.add(field)
    return cols


# API pagination
def load_paginated(
    url: str,
    desired_columns: list[str],
    *,
    limit: int = 50_000,
    max_rows: int = 250_000,
    order_by: Optional[str] = None,
    timeout: int = 60,
) -> pd.DataFrame:

    cols = discover_columns(url, timeout=timeout)
    if not cols:
        return pd.DataFrame()

    select_fields = [c for c in desired_columns if c in cols]
    if not select_fields:
        select_fields = [sorted(list(cols))[0]]

    base_params: dict[str, str] = {"$select": ", ".join(select_fields)}
    if order_by and order_by in cols:
        base_params["$order"] = f"{order_by} DESC"

    all_records: list[dict] = []
    offset = 0

    while True:
        params = {**base_params, "$limit": limit, "$offset": offset}
        r = requests.get(url, params=params, timeout=timeout)
        r.raise_for_status()
        block = r.json()

        if not block:
            break

        all_records.extend(block)
        offset += limit

        if len(all_records) >= max_rows:
            all_records = all_records[:max_rows]
            break

    return pd.json_normalize(all_records)


# chage borough code to borough name
def map_borough(df: pd.DataFrame, borough_col: str = "borough") -> pd.DataFrame:
    df2 = df.copy()
    if borough_col in df2.columns:
        df2[borough_col] = df2[borough_col].map(borough_code).fillna(df2[borough_col])
    return df2


# filter last 12 months
def filter_last_12_months(
    df: pd.DataFrame,
    date_col: str,
    *,
    today: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:

    df2 = df.copy()
    if date_col not in df2.columns:
        return df2.iloc[0:0].copy()

    df2[date_col] = pd.to_datetime(df2[date_col], errors="coerce")
    df2 = df2.dropna(subset=[date_col])

    if today is None:
        today = pd.Timestamp.today().normalize()
    else:
        today = pd.Timestamp(today).normalize()

    last_year = today - pd.DateOffset(years=1)
    return df2[df2[date_col].between(last_year, today)]


# applying other filters
def apply_filter(
    df: pd.DataFrame,
    *,
    borough_col: Optional[str] = None,
    selected_borough: Optional[list[str]] = None,
    type_col: Optional[str] = None,
    selected_types: Optional[list[str]] = None,
    status_col: Optional[str] = None,
    selected_status: Optional[list[str]] = None,
    today: Optional[pd.Timestamp] = None,
) -> pd.DataFrame:
    out = df.copy()

    if borough_col and borough_col in out.columns and selected_borough:
        out = out[out[borough_col].astype(str).isin(selected_borough)]

    if type_col and type_col in out.columns and selected_types:
        out = out[out[type_col].astype(str).isin(selected_types)]

    if status_col and status_col in out.columns and selected_status:
        out = out[out[status_col].astype(str).isin(selected_status)]

    return out


def permit_timeseries_by_borough(
    df: pd.DataFrame,
    *,
    date_col: str,
    borough_col: str = "borough",
    status_col: Optional[str] = None,
    approved_values: tuple[str, ...] = ("APPROVED", "ISSUED"),
    freq: str = "MS",
) -> pd.DataFrame:
    df2 = df.copy()

    if date_col not in df2.columns or borough_col not in df2.columns:
        return pd.DataFrame({"Period": [], "Borough": [], "Count": []})

    df2[date_col] = pd.to_datetime(df2[date_col], errors="coerce")
    df2 = df2.dropna(subset=[date_col])

    if status_col and status_col in df2.columns:
        allowed = {v.upper() for v in approved_values}
        s = df2[status_col].astype(str).str.upper()
        df2 = df2[s.isin(allowed)]

    out = (
        df2.groupby([pd.Grouper(key=date_col, freq=freq), borough_col])
        .size()
        .reset_index(name="Count")
        .rename(columns={date_col: "Period", borough_col: "Borough"})
        .sort_values(["Period", "Borough"])
        .reset_index(drop=True)
    )
    return out
