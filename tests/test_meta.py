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


def test_stat_pushdown():
    from deltalake2db.delta_meta_retrieval import get_meta, PolarsEngine

    path = "tests/data/data-skipping-basic-stats-all-types-columnmapping-name"
    m = get_meta(PolarsEngine(None), path)
    assert len(list(m.get_add_actions_filtered())) == 1
    assert len(list(m.get_add_actions_filtered([("as_int", "=", 0)]))) == 1
    assert len(list(m.get_add_actions_filtered([("as_int", "=", 2)]))) == 1


def test_filtering():
    from deltalake2db.delta_meta_retrieval import get_meta, PolarsEngine

    dt = "tests/data/data-reader-partition-values"
    m = get_meta(PolarsEngine(None), dt)
    assert len(list(m.get_add_actions_filtered())) == 3
    assert (
        len(
            list(
                m.get_add_actions_filtered(
                    [("as_date", "=", date.fromisoformat("2021-09-08"))]
                )
            )
        )
        == 2
    )
    assert (
        len(
            list(
                m.get_add_actions_filtered(
                    [("as_date", "<>", date.fromisoformat("2021-09-08"))]
                )
            )
        )
        == 1
    )
    assert (
        len(
            list(
                m.get_add_actions_filtered(
                    [("as_date", "=", date.fromisoformat("2025-09-08"))]
                )
            )
        )
        == 0
    )

    assert len(list(m.get_add_actions_filtered([("as_string", "=", None)]))) == 1
    assert (
        len(list(m.get_add_actions_filtered([("as_string", "in", [None, "0asdf2"])])))
        == 1
    )
    assert len(list(m.get_add_actions_filtered([("as_string", "=", "0asfd")]))) == 0
    assert len(list(m.get_add_actions_filtered([("as_string", "<>", "0asfd")]))) == 3
    assert len(list(m.get_add_actions_filtered([("as_string", "<>", None)]))) == 2
    assert len(list(m.get_add_actions_filtered([("as_int", ">=", "-1")]))) == 2
    assert len(list(m.get_add_actions_filtered([("as_int", ">", 0)]))) == 1
    assert len(list(m.get_add_actions_filtered([("as_int", ">", 1)]))) == 0
    assert len(list(m.get_add_actions_filtered([("as_int", ">=", 1)]))) == 1
