from deltalake2db.delta_meta_retrieval import get_meta, PolarsEngine
from datetime import datetime, timezone


def test_last_write_time():
    import deltalake

    meta = get_meta(PolarsEngine(None), "tests/data/user")
    assert meta.last_write_time is not None
    dt = deltalake.DeltaTable("tests/data/user")

    t2 = datetime.fromtimestamp(
        dt.history(1)[-1]["timestamp"] / 1000.0,
        tz=timezone.utc,
    )
    assert meta.last_write_time == t2
