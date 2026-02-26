# important: permit_page.py functions is outside the test file, run test by typing ```python -m pytest -q````

import pandas as pd

import functions.permit_page as p


def test_first_column_found():
    df = pd.DataFrame({"a": [1], "b": [2]})
    assert p.first_column(df, ["x", "b", "a"]) == "b"


def test_first_column_none():
    df = pd.DataFrame({"a": [1]})
    assert p.first_column(df, ["x", "y"]) is None


def test_map_borough_codes():
    df = pd.DataFrame({"borough": [1, "2", 3, "4", 5, 9, None]})
    out = p.map_borough(df, "borough")
    assert out["borough"].tolist() == [
        "Manhattan",
        "Bronx",
        "Brooklyn",
        "Queens",
        "Staten Island",
        9,
        None,
    ]


def test_filter_last_12_months_edges():
    today = pd.Timestamp("2026-02-25")
    df = pd.DataFrame(
        {
            "issued_date": [
                "2025-02-25",
                "2025-02-24",
                "2026-02-25",
                "bad-date",
            ],
            "x": [1, 2, 3, 4],
        }
    )
    out = p.filter_last_12_months(df, "issued_date", today=today)
    assert out["x"].tolist() == [1, 3]


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
    assert ts.shape[0] == 2
    assert ts["Count"].tolist() == [2, 1]


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
    assert ts["Count"].tolist() == [2]


def test_permit_timeseries_missing_date():
    df = pd.DataFrame({"borough": ["Manhattan"]})
    ts = p.permit_timeseries_by_borough(
        df, date_col="issued_date", borough_col="borough"
    )
    assert ts.empty
