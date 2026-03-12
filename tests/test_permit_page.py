# important: permit_page.py functions is outside the test file.
# run test with:
# python -m pytest -q

import pandas as pd

import functions.permit_page as p

# ── first_column ─────────────────────────────────────────────────────────────


def test_first_column_found():
    df = pd.DataFrame({"a": [1], "b": [2]})
    assert p.first_column(df, ["x", "b", "a"]) == "b"


def test_first_column_none():
    df = pd.DataFrame({"a": [1]})
    assert p.first_column(df, ["x", "y"]) is None


def test_first_column_empty_candidates():
    df = pd.DataFrame({"a": [1]})
    assert p.first_column(df, []) is None


# ── filter_last_12_months ─────────────────────────────────────────────────────


def test_filter_last_12_months_edges():
    today = pd.Timestamp("2026-02-25")
    df = pd.DataFrame(
        {
            "issued_date": [
                "2025-02-25",  # exactly 12 months ago → included
                "2025-02-24",  # just outside → excluded
                "2026-02-25",  # today → included
                "bad-date",  # unparseable → dropped
            ],
            "x": [1, 2, 3, 4],
        }
    )
    out = p.filter_last_12_months(df, "issued_date", today=today)
    assert out["x"].tolist() == [1, 3]


def test_filter_last_12_months_missing_col():
    """Returns empty df when date column is absent."""
    df = pd.DataFrame({"x": [1, 2]})
    out = p.filter_last_12_months(df, "issued_date")
    assert out.empty


def test_filter_last_12_months_all_bad_dates():
    df = pd.DataFrame({"issued_date": ["not-a-date", "also-bad"], "x": [1, 2]})
    out = p.filter_last_12_months(df, "issued_date")
    assert out.empty


# ── apply_filter ──────────────────────────────────────────────────────────────


def test_apply_filter_borough():
    df = pd.DataFrame({"borough": ["Manhattan", "Bronx", "Brooklyn"]})
    out = p.apply_filter(df, borough_col="borough", selected_borough=["Manhattan", "Bronx"])
    assert out["borough"].tolist() == ["Manhattan", "Bronx"]


def test_apply_filter_type():
    df = pd.DataFrame({"work_type": ["NB", "A1", "DM"]})
    out = p.apply_filter(df, type_col="work_type", selected_types=["NB", "DM"])
    assert out["work_type"].tolist() == ["NB", "DM"]


def test_apply_filter_status():
    df = pd.DataFrame({"permit_status": ["ISSUED", "PENDING", "APPROVED"]})
    out = p.apply_filter(df, status_col="permit_status", selected_status=["ISSUED", "APPROVED"])
    assert out["permit_status"].tolist() == ["ISSUED", "APPROVED"]


def test_apply_filter_no_filters():
    """With no filters passed, returns all rows unchanged."""
    df = pd.DataFrame({"borough": ["Manhattan", "Bronx"]})
    out = p.apply_filter(df)
    expected_rows = 2
    assert len(out) == expected_rows


# ── permit_timeseries_by_borough ──────────────────────────────────────────────


def test_permit_timeseries_month():
    df = pd.DataFrame(
        {
            "issued_date": ["2026-01-05", "2026-01-20", "2026-02-02"],
            "borough": ["Manhattan", "Manhattan", "Bronx"],
        }
    )
    ts = p.permit_timeseries_by_borough(
        df, date_col="issued_date", borough_col="borough", status_col=None, freq="MS"
    )
    expected_rows = 2
    assert ts.shape[0] == expected_rows


def test_permit_timeseries_status():
    df = pd.DataFrame(
        {
            "issued_date": ["2026-01-05", "2026-01-20", "2026-01-25"],
            "borough": ["Manhattan", "Manhattan", "Manhattan"],
            "permit_status": ["ISSUED", "PENDING", "APPROVED"],
        }
    )
    ts = p.permit_timeseries_by_borough(
        df,
        date_col="issued_date",
        borough_col="borough",
        status_col="permit_status",
        approved_values=("APPROVED", "ISSUED"),
        freq="MS",
    )
    assert ts["Count"].tolist() == [2]  # PENDING excluded


def test_permit_timeseries_missing_date():
    df = pd.DataFrame({"borough": ["Manhattan"]})
    ts = p.permit_timeseries_by_borough(df, date_col="issued_date", borough_col="borough")
    assert ts.empty


def test_permit_timeseries_missing_borough():
    df = pd.DataFrame({"issued_date": ["2026-01-05"]})
    ts = p.permit_timeseries_by_borough(df, date_col="issued_date", borough_col="borough")
    assert ts.empty


def test_permit_timeseries_bad_dates_dropped():
    """Unparseable dates are dropped silently, valid ones still count."""
    df = pd.DataFrame(
        {
            "issued_date": ["2026-01-05", "not-a-date"],
            "borough": ["Manhattan", "Bronx"],
        }
    )
    ts = p.permit_timeseries_by_borough(
        df, date_col="issued_date", borough_col="borough", status_col=None, freq="MS"
    )
    assert ts["Count"].tolist() == [1]
