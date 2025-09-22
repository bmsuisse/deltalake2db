from collections import OrderedDict
import duckdb
import polars as pl
import pytest


@pytest.mark.parametrize("use_delta_ext", [False, True])
def test_col_mapping(use_delta_ext):
    dt = "tests/data/faker2"

    from deltalake2db import get_sql_for_delta

    if (
        use_delta_ext
    ):  # see https://github.com/duckdb/duckdb-delta/issues/50#issuecomment-2730119786
        select = ["Super Name", "Company Very Short", "age", "new_name"]
    else:
        select = None
    with duckdb.connect() as con:
        sql = get_sql_for_delta(
            dt, duck_con=con, use_delta_ext=use_delta_ext, select=select
        )
        print(sql)
        con.execute("create view delta_table as " + sql)

        df = pl.from_arrow(con.execute("select * from delta_table").fetch_arrow_table())
        assert isinstance(df, pl.DataFrame)
        print(df)
    if not use_delta_ext:
        coord_field = df.schema["main_coord"]
        assert isinstance(coord_field, pl.Struct)
        fields = coord_field.fields
        assert "lat" in [f.name for f in fields]
        assert "lon" in [f.name for f in fields]

    age_field = df.schema["age"]
    assert isinstance(age_field, pl.List)
    assert isinstance(age_field.inner, pl.Int64)
    if not use_delta_ext:
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
    else:
        assert df.schema == OrderedDict(
            [
                ("Super Name", pl.String),
                ("Company Very Short", pl.String),
                ("age", pl.List(pl.Int64)),
                ("new_name", pl.String),
            ]
        )

    as_py_rows = df.rows(named=True)
    print(as_py_rows)


@pytest.mark.parametrize("use_delta_ext", [False, True])
def test_strange_cols(use_delta_ext):
    dt = "tests/data/user"

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, dt, "delta_table", use_delta_ext=use_delta_ext
        )
        con.execute("select * from delta_table")
        assert con.description is not None
        col_names = [c[0] for c in con.description]
        assert "time stämp" in col_names
        print("\n")
        print(con.df())


def test_filter_number():
    dt = "tests/data/user"

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt, "delta_table", conditions={"Age": 23.0})
        con.execute("select FirstName from delta_table")
        assert con.description is not None
        col_names = [c[0] for c in con.description]
        assert col_names == ["FirstName"]
        names = con.fetchall()
        assert len(names) == 1
        assert names[0][0] == "Peter"
    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt, "delta_table_1", conditions={"Age": 23.0})
        con.execute("select * from delta_table_1")
        assert con.description is not None
        col_types_1 = [c[1] for c in con.description]
        duckdb_create_view_for_delta(
            con, dt, "delta_table_2", conditions={"Age": 500.0}
        )
        con.execute("select * from delta_table_2")
        assert con.description is not None
        col_types_2 = [c[1] for c in con.description]
        assert col_types_1 == col_types_2


def test_filter_name():
    dt = "tests/data/user"

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(
            con, dt, "delta_table", conditions={"FirstName": "Peter"}
        )
        con.execute("select FirstName from delta_table")
        assert con.description is not None
        col_names = [c[0] for c in con.description]
        names = con.fetchall()
        assert len(names) == 1
        assert names[0][0] == "Peter"


def test_user_empty():
    dt = "tests/data/user_empty"

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt, "delta_table")
        con.execute("select * from delta_table")
        assert con.description is not None
        col_names = [c[0] for c in con.description]
        assert "time stämp" in col_names
        assert len(con.fetchall()) == 0


def test_user_add():
    import shutil
    import pandas as pd
    from deltalake import DeltaTable

    shutil.rmtree("tests/data/_user2", ignore_errors=True)
    shutil.copytree("tests/data/user", "tests/data/_user2")

    dt = DeltaTable("tests/data/_user2")
    old_version = dt.version()
    from deltalake.writer import write_deltalake
    from deltalake import __version__

    if __version__.startswith("0."):
        engine_args = {"engine": "rust"}
    else:
        engine_args = {}
    write_deltalake(
        dt,
        pd.DataFrame({"User - iD": [1555], "FirstName": ["Hansueli"]}),
        schema_mode="merge",
        mode="append",
        **engine_args,  # type: ignore
    )
    dt.update_incremental()

    dt_o = DeltaTable("tests/data/_user2")
    dt_o.load_as_version(old_version)

    from deltalake2db import duckdb_create_view_for_delta

    with duckdb.connect() as con:
        duckdb_create_view_for_delta(con, dt.table_uri, "delta_table_n")
        duckdb_create_view_for_delta(con, dt_o.table_uri, "delta_table_o")
        con.execute(
            'select "User - iD" from delta_table_n except select "User - iD" from delta_table_o'
        )
        res = con.fetchall()
        assert len(res) == 1
        assert res[0][0] == 1555


def test_empty_struct():
    # >>> duckdb.execute("""Select { 'lat': 1 } as tester union all select Null""").fetchall()
    import pyarrow as pa
    import pyarrow.compute as pac

    dt = "tests/data/faker2"

    from deltalake2db import get_sql_for_delta

    with duckdb.connect() as con:
        sql = get_sql_for_delta(dt, duck_con=con)
        print(sql)
        con.execute("create view delta_table as " + sql)

        df = con.execute("select * from delta_table").fetch_arrow_table()
        print(df)
        mc = (
            df.filter(pac.field("new_name") == "Hans Heiri")
            .select(["main_coord"])
            .to_pylist()
        )
        assert len(mc) == 1
        assert mc[0]["main_coord"] is None
