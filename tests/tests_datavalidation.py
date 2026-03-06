import pandas as pd
import pandera.pandas as pa
from pandera import Check

# creating the validation schema for 2_Building_Eviction dataset
# used gen AI to help with understanding how to structure the schema

evictions_schema = pa.DataFrameSchema(
    {
        "court_index_number": pa.Column(
            str,
            nullable=False,
        ),
        "executed_date": pa.Column(
            pa.DateTime,
            nullable=False,
            checks=[
                Check.ge(pd.Timestamp("2025-01-01")),
                Check.le(pd.Timestamp.today()),
            ],
        ),
        "borough": pa.Column(
            str,
            nullable=False,
            checks=Check.isin(["MANHATTAN", "BRONX", "BROOKLYN", "QUEENS", "STATEN ISLAND"]),
        ),
        "residential_commercial_ind": pa.Column(
            str,
            nullable=False,
            checks=Check.isin(["Residential", "Commercial", "R", "C"]),
        ),
        "latitude": pa.Column(
            float,
            nullable=True,
            checks=[Check.ge(40.0), Check.le(41.5)],
        ),
        "longitude": pa.Column(
            float,
            nullable=True,
            checks=[Check.ge(-75.0), Check.le(-73.0)],
        ),
        "community_board": pa.Column(
            int,
            nullable=True,
            checks=[Check.ge(1), Check.le(18)],
        ),
        "council_district": pa.Column(
            int,
            nullable=True,
            checks=[Check.ge(1), Check.le(51)],
        ),
    }
)


def test_evictions_schema():
    df = pd.DataFrame(
        {
            "court_index_number": [
                "12345/2025",
                "22345/2025",
                "32345/2025",
                "42345/2025",
            ],
            "executed_date": pd.to_datetime(
                ["2025-01-15", "2025-02-01", "2025-02-15", "2025-03-01"]
            ),
            "borough": ["BROOKLYN", "QUEENS", "BRONX", "MANHATTAN"],
            "residential_commercial_ind": ["Residential", "Commercial", "R", "C"],
            "latitude": [40.81, 40.73, 40.85, 40.58],
            "longitude": [-73.95, -73.79, -73.91, -74.15],
            "community_board": [9, 2, 5, 1],
            "council_district": [7, 26, 14, 49],
        }
    )

    evictions_schema.validate(df)


# Used pytest and it passed using tests/datavalidation.py

# Write up: We are still deciding which key variables/columns from the eviction dataset are
# of interest to us, but we chose court index number, executed date, borough, residential/commercial,
# community board, council district and the coordinates.

# We assumed that the court index numbers are present, the dates are in 2025,
# bourghs are NY boroughs, and consistent building type categories.
