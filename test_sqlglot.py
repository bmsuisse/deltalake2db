import sqlglot as sg


print(
    repr(
        sg.parse_one(
            "select map_from_entries([('a', 1)]) as tester",
            dialect="duckdb",
        )
    )
)
