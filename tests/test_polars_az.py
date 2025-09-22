from collections import OrderedDict
from deltalake import DeltaTable
import polars as pl
import pytest

from deltalake2db.polars import PolarsSettings


@pytest.mark.skip(reason="Would need a real account for this")
def test_chain():
    opts = {"use_emulator": "True", "chain": "default"}
    from deltalake2db import polars_scan_delta

    df = polars_scan_delta("az://testlakedb/td/delta/fake", storage_options=opts)

    df = df.collect()

    main_coord_field = df.schema["main_coord"]
    assert isinstance(main_coord_field, pl.Struct)
    fields = main_coord_field.fields
    assert "lat" in [f.name for f in fields]
    assert "lon" in [f.name for f in fields]

    age_field = df.schema["age"]
    assert isinstance(age_field, pl.List)
    assert isinstance(age_field.inner, pl.Int64)
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


@pytest.mark.parametrize("use_pyarrow", [True, False])
def test_col_mapping(storage_options, use_pyarrow):
    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(
        "az://testlakedb/td/delta/fake",
        storage_options=storage_options,
        settings=PolarsSettings(
            use_pyarrow=use_pyarrow,
        ),
    )
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    coord_field = df.schema["main_coord"]
    assert isinstance(coord_field, pl.Struct)
    fields = coord_field.fields
    assert "lat" in [f.name for f in fields]
    assert "lon" in [f.name for f in fields]

    age_field = df.schema["age"]
    assert isinstance(age_field, pl.List)
    assert isinstance(age_field.inner, pl.Int64)
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
    from deltalake2db import polars_scan_delta

    df = polars_scan_delta(
        "az://testlakedb/td/delta/fake", storage_options=storage_options
    )

    df = df.collect()

    mc = df.filter(new_name="Hans Heiri").select("main_coord").to_dicts()
    assert len(mc) == 1
    assert mc[0]["main_coord"] is None
