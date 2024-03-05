from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
from deltalake import DeltaTable, Field, DataType
from deltalake.schema import StructType, ArrayType
import os


def _get_expr(
    base_expr: "pl.Expr|None", dtype: "DataType", meta: Field | None
) -> "pl.Expr":
    import polars as pl

    pn = (
        meta.metadata.get("delta.columnMapping.physicalName", meta.name)
        if meta
        else None
    )
    if base_expr is None:
        assert pn is not None
        base_expr = pl.col(pn)
    if isinstance(dtype, StructType):
        return pl.struct(
            *[
                _get_expr(
                    base_expr.struct.field(
                        subfield.metadata.get(
                            "delta.columnMapping.physicalName", subfield.name
                        )
                    ),
                    subfield.type,
                    subfield,
                )
                for subfield in dtype.fields
            ]
        ).alias(meta.name)
    elif isinstance(dtype, ArrayType):
        return base_expr.list.eval(
            _get_expr(pl.element(), dtype=dtype.element_type, meta=meta)
        ).alias(meta.name)
    return base_expr.alias(meta.name) if meta else base_expr


def scan_delta_union(delta_table: DeltaTable) -> "pl.LazyFrame":
    import polars as pl

    all_ds = []
    all_fields = delta_table.schema().fields
    for ac in delta_table.get_add_actions(flatten=True).to_pylist():
        fullpath = os.path.join(delta_table.table_uri, ac["path"])
        base_ds = pl.scan_parquet(
            fullpath, storage_options=delta_table._storage_options
        )
        parquet_schema = base_ds.limit(0).schema
        selects = []
        for field in all_fields:
            pn = field.metadata.get("delta.columnMapping.physicalName", field.name)
            if "partition_values" in ac and pn in ac["partition_values"]:
                part_vl = ac["partition_values"][pn]
                selects.append(pl.lit(part_vl).alias(field.name))
            elif "partition." + pn in ac:
                part_vl = ac["partition." + pn]
                selects.append(pl.lit(part_vl).alias(field.name))
            elif "partition_values" in ac and field.name in ac["partition_values"]:
                # as of delta 0.14
                part_vl = ac["partition_values"][field.name]
                selects.append(pl.lit(part_vl).alias(field.name))
            elif "partition." + field.name in ac:
                # as of delta 0.14
                part_vl = ac["partition." + field.name]
                selects.append(pl.lit(part_vl).alias(field.name))
            elif (
                field.metadata.get("delta.columnMapping.physicalName", field.name)
                in parquet_schema
            ):
                selects.append(_get_expr(None, field.type, field))

        ds = base_ds.select(*selects)
        all_ds.append(ds)
    return pl.concat(all_ds, how="diagonal_relaxed")
