import sqlglot as sg


print(
    repr(
        sg.parse_one(
            "select list_transform(x_0 -> struct_pack(i := 4, s := 'string')), c.b",
            dialect="duckdb",
        )
    )
)
