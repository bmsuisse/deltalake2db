from collections import OrderedDict
from deltalake import DeltaTable
import polars as pl
import pytest


def test_col_mapping():
    dt = DeltaTable("tests/data/faker2")

    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(dt)

    df = df.collect()

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


def test_user_add():
    import shutil
    import pandas as pd

    shutil.rmtree("tests/data/_user3", ignore_errors=True)
    shutil.copytree("tests/data/user", "tests/data/_user3")
    dt = DeltaTable("tests/data/_user3")
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

    dt_o = DeltaTable("tests/data/_user3")
    dt_o.load_as_version(old_version)

    from deltalake2db import polars_scan_delta
    import polars as pl

    nc = polars_scan_delta(dt).select(pl.col("User - iD")).collect().to_dicts()
    oc = polars_scan_delta(dt_o).select(pl.col("User - iD")).collect().to_dicts()
    diff = [o["User - iD"] for o in nc if o not in oc]
    assert diff == [1555]


def test_user_empty():
    dt = DeltaTable("tests/data/user_empty")

    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(dt).collect()
    assert df.shape[0] == 0
    assert "time stämp" in df.columns


def test_strange_cols():
    dt = DeltaTable("tests/data/user")

    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(dt)

    df = df.collect()
    col_names = df.columns
    assert "time stämp" in col_names


def test_filter_number():
    dt = DeltaTable("tests/data/user")

    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(dt, conditions={"Age": 23.0})
    res = df.collect().to_dicts()
    assert len(res) == 1
    assert res[0]["FirstName"] == "Peter"


def test_filter_name():
    dt = DeltaTable("tests/data/user")

    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(dt, conditions={"FirstName": "Peter"})
    res = df.collect().to_dicts()
    assert len(res) == 1
    assert res[0]["FirstName"] == "Peter"


def test_schema():
    dt = DeltaTable("tests/data/user")

    from deltalake2db import polars_scan_delta, get_polars_schema

    df = polars_scan_delta(dt)
    schema = get_polars_schema(dt)

    assert df.schema == schema


@pytest.mark.skip(reason="Polars reads null structs as structs, so no luck")
def test_empty_struct():
    dt = DeltaTable("tests/data/faker2")

    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(dt)

    df = df.collect()

    mc = df.filter(new_name="Hans Heiri").select("main_coord").to_dicts()
    assert len(mc) == 1
    assert mc[0]["main_coord"] is None
