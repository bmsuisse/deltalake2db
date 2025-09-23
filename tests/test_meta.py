from deltalake2db.delta_meta_retrieval import get_meta, PolarsEngine
from datetime import date, datetime, timezone


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
        assert meta.version == dt.version()


def test_filtering():
    from deltalake2db.delta_meta_retrieval import get_meta, PolarsEngine

    dt = "tests/data/data-reader-partition-values"
    m = get_meta(PolarsEngine(None), dt)
    assert len(list(m.get_add_actions_filtered())) == 3
    assert (
        len(
            list(
                m.get_add_actions_filtered(
                    {"as_date": date.fromisoformat("2021-09-08")}
                )
            )
        )
        == 2
    )
    assert (
        len(
            list(
                m.get_add_actions_filtered(
                    {"as_date": date.fromisoformat("2025-09-08")}
                )
            )
        )
        == 0
    )
