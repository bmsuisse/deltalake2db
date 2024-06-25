import sqlglot as sg
import sqlglot.expressions as ex

print(repr(ex.DataType.build("decimal(24,5)")))
print(
    repr(
        sg.parse_one(
            "select 234324::decimal(24,5) as tester",
            dialect="duckdb",
        )
    )
)
