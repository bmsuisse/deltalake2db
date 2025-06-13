from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from arro3.core import RecordBatch as arro3RecordBatch


def to_pylist(reader: "pa.RecordBatch|arro3RecordBatch"):
    if hasattr(reader, "to_pylist"):
        return reader.to_pylist()  # type: ignore
    assert not isinstance(reader, pa.RecordBatch)
    results = [{}] * reader.num_rows
    for i, c in enumerate(reader.column_names):
        array = reader.column(i)
        ls = array.to_pylist()
        for j, v in enumerate(ls):
            results[j][c] = v
    return results
