from collections import OrderedDict
from deltalake import DeltaTable
import duckdb
import polars as pl
import pytest


def test_col_mapping(storage_options):
    dt = DeltaTable("az://testlakedb/td/delta/fake", storage_options=storage_options)

    from deltalake2db import get_sql_for_delta

    with duckdb.connect() as con:

        sql = get_sql_for_delta(dt, duck_con=con)
        sql = get_sql_for_delta(
            dt, duck_con=con
        )  # do it twice to test duplicate secrets
        print(sql)
        con.execute("create view delta_table as " + sql)

        df = pl.from_arrow(con.execute("select * from delta_table").fetch_arrow_table())
        print(df)

    assert isinstance(df.schema["main_coord"], pl.Struct)
    fields = df.schema["main_coord"].fields
    assert "lat" in [f.name for f in fields]
    assert "lon" in [f.name for f in fields]

    assert isinstance(df.schema["age"], pl.List)
    assert isinstance(df.schema["age"].inner, pl.Int64)
    assert df.schema == OrderedDict(
        [
            ("Super Name", pl.String),
            ("Company Very Short", pl.String),
            ("main_coord", pl.Struct({"lat": pl.Float64, "lon": pl.Float64})),
            ("coords", pl.List(pl.Struct({"lat": pl.Float64, "lon": pl.Float64}))),
            ("age", pl.List(pl.Int64)),
            ("new_name", pl.String),
        ]
    )

    as_py_rows = df.rows(named=True)
    print(as_py_rows)


def test_empty_struct(storage_options):
    # >>> duckdb.execute("""Select { 'lat': 1 } as tester union all select Null""").fetchall()
    import pyarrow as pa

    dt = DeltaTable("az://testlakedb/td/delta/fake", storage_options=storage_options)

    from deltalake2db import get_sql_for_delta

    with duckdb.connect() as con:

        sql = get_sql_for_delta(dt, duck_con=con)
        print(sql)
        con.execute("create view delta_table as " + sql)

        df = con.execute("select * from delta_table").fetch_arrow_table()
        print(df)
        mc = (
            df.filter(pa.compute.field("new_name") == "Hans Heiri")
            .select(["main_coord"])
            .to_pylist()
        )
        assert len(mc) == 1
        assert mc[0]["main_coord"] is None
