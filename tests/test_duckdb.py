from collections import OrderedDict
from deltalake import DeltaTable
import duckdb
import polars as pl
import pytest


def test_col_mapping():
    dt = DeltaTable("tests/data/faker2")

    from deltalake2db import get_sql_for_delta

    with duckdb.connect() as con:
        sql = get_sql_for_delta(dt, duck_con=con)
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


@pytest.mark.parametrize("use_delta_ext", [False, True])
def test_strange_cols(use_delta_ext):
    dt = DeltaTable("tests/data/user")

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, dt, "delta_table", use_delta_ext=use_delta_ext
        )
        con.execute("select * from delta_table")
        col_names = [c[0] for c in con.description]
        assert "time stämp" in col_names
        print("\n")
        print(con.df())


def test_filter_number():
    dt = DeltaTable("tests/data/user")

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt, "delta_table", conditions={"Age": 23.0})
        con.execute("select FirstName from delta_table")
        col_names = [c[0] for c in con.description]
        assert col_names == ["FirstName"]
        names = con.fetchall()
        assert len(names) == 1
        assert names[0][0] == "Peter"
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt, "delta_table_1", conditions={"Age": 23.0})
        con.execute("select * from delta_table_1")
        col_types_1 = [c[1] for c in con.description]
        duckdb_create_view_for_delta(
            con, dt, "delta_table_2", conditions={"Age": 500.0}
        )
        con.execute("select * from delta_table_2")
        col_types_2 = [c[1] for c in con.description]
        assert col_types_1 == col_types_2


def test_filter_name():
    dt = DeltaTable("tests/data/user")

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, dt, "delta_table", conditions={"FirstName": "Peter"}
        )
        con.execute("select FirstName from delta_table")
        col_names = [c[0] for c in con.description]
        names = con.fetchall()
        assert len(names) == 1
        assert names[0][0] == "Peter"


def test_user_empty():
    dt = DeltaTable("tests/data/user_empty")

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt, "delta_table")
        con.execute("select * from delta_table")
        col_names = [c[0] for c in con.description]
        assert "time stämp" in col_names
        assert len(con.fetchall()) == 0


def test_user_add():
    import shutil
    import pandas as pd

    shutil.rmtree("tests/data/_user2", ignore_errors=True)
    shutil.copytree("tests/data/user", "tests/data/_user2")
    dt = DeltaTable("tests/data/_user2")
    old_version = dt.version()
    from deltalake.writer import write_deltalake

    write_deltalake(
        dt,
        pd.DataFrame({"User - iD": [1555], "FirstName": ["Hansueli"]}),
        schema_mode="merge",
        engine="rust",
        mode="append",
    )
    dt.update_incremental()

    dt_o = DeltaTable("tests/data/_user2")
    dt_o.load_as_version(old_version)

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt, "delta_table_n")
        duckdb_create_view_for_delta(con, dt_o, "delta_table_o")
        con.execute(
            'select "User - iD" from delta_table_n except select "User - iD" from delta_table_o'
        )
        res = con.fetchall()
        assert len(res) == 1
        assert res[0][0] == 1555


def test_empty_struct():
    # >>> duckdb.execute("""Select { 'lat': 1 } as tester union all select Null""").fetchall()
    import pyarrow as pa

    dt = DeltaTable("tests/data/faker2")

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
