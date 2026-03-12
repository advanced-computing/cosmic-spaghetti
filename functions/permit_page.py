# permit_page.py v2 — includes date_col

from __future__ import annotations

import re

import pandas as pd
import requests

from functions.permit_validation import permit_schema


# return first column name from dataset (anticipating changing in dataset)
def first_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    for c in candidates:
        if c in df.columns:
            return c
    return None


# discover desired columns
def discover_columns(url: str, timeout: int = 30) -> set[str]:

    m = re.search(r"/resource/([a-z0-9]{4}-[a-z0-9]{4})\.json", url)
    if not m:
        return set()

    dataset_id = m.group(1)
    meta_url = f"https://data.cityofnewyork.us/api/views/{dataset_id}.json"

    r = requests.get(meta_url, timeout=timeout)
    r.raise_for_status()
    meta = r.json()

    cols = set()
    for c in meta.get("columns", []):
        field = c.get("fieldName")
        if field:
            cols.add(field)
    return cols


# API pagination
def load_paginated(  # noqa: PLR0913
    url: str,
    desired_columns: list[str] | None = None,
    *,
    limit: int = 20_000,
    max_rows: int = 250_000,
    order_by: str = "issued_date",
    date_col: str = "issued_date",
    today: pd.Timestamp | None = None,
    timeout: int = 60,
) -> pd.DataFrame:
    if desired_columns is None:
        desired_columns = ["issued_date", "borough", "permit_type", "job_status"]

    cols = discover_columns(url, timeout=timeout)
    if not cols:
        return pd.DataFrame()

    select_fields = [c for c in desired_columns if c in cols]
    if not select_fields:
        return pd.DataFrame()

    today = pd.Timestamp.today().normalize() if today is None else pd.Timestamp(today).normalize()
    last_year = today - pd.DateOffset(years=1)
    last_year_str = last_year.strftime("%Y-%m-%dT00:00:00")
    today_str = today.strftime("%Y-%m-%dT23:59:59")

    base_params: dict[str, str] = {
        "$select": ", ".join(select_fields),
        "$where": f"{date_col} between '{last_year_str}' and '{today_str}'",
    }

    if order_by in cols:
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

    df = pd.json_normalize(all_records)
    df = permit_schema.validate(df)
    return df


# filter last 12 months
def filter_last_12_months(
    df: pd.DataFrame,
    date_col: str,
    *,
    today: pd.Timestamp | None = None,
) -> pd.DataFrame:

    df2 = df.copy()
    if date_col not in df2.columns:
        return df2.iloc[0:0].copy()

    df2[date_col] = pd.to_datetime(df2[date_col], errors="coerce")
    df2 = df2.dropna(subset=[date_col])

    today = pd.Timestamp.today().normalize() if today is None else pd.Timestamp(today).normalize()

    last_year = today - pd.DateOffset(years=1)
    return df2[df2[date_col].between(last_year, today)]


# applying other filters
def apply_filter(  # noqa: PLR0913
    df: pd.DataFrame,
    *,
    borough_col: str | None = None,
    selected_borough: list[str] | None = None,
    type_col: str | None = None,
    selected_types: list[str] | None = None,
    status_col: str | None = None,
    selected_status: list[str] | None = None,
    today: pd.Timestamp | None = None,
) -> pd.DataFrame:
    out = df.copy()

    if borough_col and borough_col in out.columns and selected_borough:
        out = out[out[borough_col].astype(str).isin(selected_borough)]

    if type_col and type_col in out.columns and selected_types:
        out = out[out[type_col].astype(str).isin(selected_types)]

    if status_col and status_col in out.columns and selected_status:
        out = out[out[status_col].astype(str).isin(selected_status)]

    return out


def permit_timeseries_by_borough(  # noqa: PLR0913
    df: pd.DataFrame,
    *,
    date_col: str,
    borough_col: str = "borough",
    status_col: str | None = None,
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
