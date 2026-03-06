from pandera import Column, DataFrameSchema

permit_schema = DataFrameSchema(
    {
        "borough": Column(object, nullable=True),
        "issued_date": Column(object, nullable=True),
        "approved_date": Column(object, nullable=True),
        "expired_date": Column(object, nullable=True),
        "work_type": Column(object, nullable=True),
        "permit_status": Column(object, nullable=True),
        "community_board": Column(object, nullable=True),
    },
    coerce=False,
)
