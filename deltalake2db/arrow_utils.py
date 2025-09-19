from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from arro3.core import RecordBatch as arro3RecordBatch


def to_pylist(reader: "pa.RecordBatch|arro3RecordBatch"):
    if hasattr(reader, "to_pylist"):
        return reader.to_pylist()  # type: ignore

    try:
        import pyarrow as pa

        return pa.record_batch(reader).to_pylist()
    except ImportError:
        pass
    results = [{}] * reader.num_rows
    for i, c in enumerate(reader.column_names):
        array = reader.column(i)
        ls = array.to_pylist()  # this is still not implemented in arro3
        for j, v in enumerate(ls):
            results[j][c] = v
    return results
