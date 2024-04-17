from collections import OrderedDict
from deltalake import DeltaTable
import polars as pl
import pytest


@pytest.mark.skip(reason="Would need a real account for this")
def test_chain():
    opts = {"use_emulator": "True", "chain": "default"}
    from deltalake2db import polars_scan_delta

    df = polars_scan_delta("az://testlakedb/td/delta/fake", storage_options=opts)

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


def test_col_mapping(storage_options):
    dt = DeltaTable("az://testlakedb/td/delta/fake", storage_options=storage_options)

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


@pytest.mark.skip(reason="Polars reads null structs as structs, so no luck")
def test_empty_struct(storage_options):
    dt = DeltaTable("az://testlakedb/td/delta/fake", storage_options=storage_options)

    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(dt)

    df = df.collect()

    mc = df.filter(new_name="Hans Heiri").select("main_coord").to_dicts()
    assert len(mc) == 1
    assert mc[0]["main_coord"] is None
