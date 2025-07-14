from deltalake import write_deltalake
import polars as pl
import pytest
from polars.testing import assert_frame_equal
from deltalake2db import polars_scan_delta
from datetime import datetime


@pytest.fixture()
def data_batch_1():
    return pl.DataFrame(
        {
            "foo": [1, 2, 3, 4, 5, 6, 7, 8, 9],
            "bar": ["1", "2", "3", "4", "5", "6", "7", "8", "9"],
            "date_month": [
                201001,
                201002,
                201003,
                201004,
                201005,
                201006,
                201007,
                201008,
                201009,
            ],
            "datetime": [
                datetime.fromisoformat("2010-01-01"),
                datetime.fromisoformat("2010-02-01"),
                datetime.fromisoformat("2010-03-01"),
                datetime.fromisoformat("2010-04-01"),
                datetime.fromisoformat("2010-05-01"),
                datetime.fromisoformat("2010-06-01"),
                datetime.fromisoformat("2010-07-01"),
                datetime.fromisoformat("2010-08-01"),
                datetime.fromisoformat("2010-09-01"),
            ],
            "static_part": ["A", "A", "A", "B", "B", "B", "C", "C", "C"],
        }
    )


def test_roundtrip_read(tmp_path, data_batch_1: pl.DataFrame):
    write_deltalake(
        tmp_path,
        data_batch_1.to_arrow(),
        mode="append",
    )

    result = polars_scan_delta(tmp_path).collect()

    assert_frame_equal(result, data_batch_1)

    write_deltalake(tmp_path, data_batch_1.to_arrow(), mode="append")
    result = polars_scan_delta(tmp_path).collect()
    assert_frame_equal(result, pl.concat([data_batch_1] * 2))


def test_roundtrip_read_filter(tmp_path, data_batch_1: pl.DataFrame):
    write_deltalake(
        tmp_path,
        data_batch_1.to_arrow(),
        mode="append",
    )

    result = polars_scan_delta(tmp_path).filter(pl.col("foo") > 5).collect()

    assert_frame_equal(result, data_batch_1.filter(pl.col("foo") > 5))

    write_deltalake(tmp_path, data_batch_1.to_arrow(), mode="append")
    result = polars_scan_delta(tmp_path).filter(pl.col("foo") > 5).collect()
    assert_frame_equal(result, pl.concat([data_batch_1] * 2).filter(pl.col("foo") > 5))


def test_roundtrip_read_partitioned(tmp_path, data_batch_1: pl.DataFrame):
    write_deltalake(
        tmp_path,
        data_batch_1.to_arrow(),
        mode="append",
        partition_by=["date_month", "static_part"],
    )

    result = polars_scan_delta(tmp_path).collect()

    assert_frame_equal(result, data_batch_1, check_row_order=False)

    write_deltalake(
        tmp_path,
        data_batch_1.to_arrow(),
        mode="append",
    )
    result = polars_scan_delta(tmp_path).collect()
    assert_frame_equal(result, pl.concat([data_batch_1] * 2), check_row_order=False)


def test_roundtrip_read_partitioned_filtered(tmp_path, data_batch_1: pl.DataFrame):
    write_deltalake(
        tmp_path,
        data_batch_1.to_arrow(),
        mode="append",
        partition_by=["date_month", "static_part"],
    )

    result = (
        polars_scan_delta(tmp_path)
        .filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        )
        .collect()
    )

    assert_frame_equal(
        result,
        data_batch_1.filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        ),
        check_row_order=False,
    )

    write_deltalake(tmp_path, data_batch_1.to_arrow(), mode="append")
    result = (
        polars_scan_delta(tmp_path)
        .filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        )
        .collect()
    )
    assert_frame_equal(
        result,
        pl.concat([data_batch_1] * 2).filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        ),
        check_row_order=False,
    )


def test_roundtrip_read_partitioned_filtered_select(
    tmp_path, data_batch_1: pl.DataFrame
):
    write_deltalake(
        tmp_path,
        data_batch_1.to_arrow(),
        mode="append",
        partition_by=["date_month", "static_part"],
    )

    result = (
        polars_scan_delta(tmp_path)
        .filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        )
        .select("foo")
        .collect()
    )

    assert_frame_equal(
        result,
        data_batch_1.filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        ).select("foo"),
        check_row_order=False,
    )

    write_deltalake(
        tmp_path,
        data_batch_1.to_arrow(),
        mode="append",
    )
    result = (
        polars_scan_delta(tmp_path)
        .filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        )
        .select("foo")
        .collect()
    )
    assert_frame_equal(
        result,
        pl.concat([data_batch_1] * 2)
        .filter(
            (pl.col("static_part") == "A")
            & (pl.col("date_month").is_in([201001, 201002]))
        )
        .select("foo"),
        check_row_order=False,
    )
