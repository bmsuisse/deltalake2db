from deltalake2db.delta_meta_retrieval import get_meta, PolarsEngine
from datetime import datetime, timezone


def test_last_write_time():
    import deltalake

    for tbl in ["user"]:
        meta = get_meta(PolarsEngine(None), f"tests/data/{tbl}")
        assert meta.last_write_time is not None
        dt = deltalake.DeltaTable(f"tests/data/{tbl}")

        t2 = datetime.fromtimestamp(
            dt.history(1)[-1]["timestamp"] / 1000.0,
            tz=timezone.utc,
        )
        assert meta.last_write_time == t2
