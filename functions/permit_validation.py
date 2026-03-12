from pandera import Column, DataFrameSchema

permit_schema = DataFrameSchema(
    {
        "borough": Column(object, nullable=True, required=False),
        "issued_date": Column(object, nullable=True, required=False),
        "approved_date": Column(object, nullable=True, required=False),
        "expired_date": Column(object, nullable=True, required=False),
        "work_type": Column(object, nullable=True, required=False),
        "permit_status": Column(object, nullable=True, required=False),
        "community_board": Column(object, nullable=True, required=False),
    },
    coerce=False,
    # allow extra or missing columns
    strict=False,
)
