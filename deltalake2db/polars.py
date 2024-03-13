from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import polars as pl
from deltalake import DeltaTable, Field, DataType
from deltalake.exceptions import DeltaProtocolError
from deltalake.schema import StructType, ArrayType, PrimitiveType
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

        struct_vl = (
            pl.when(base_expr.is_null())
            .then(pl.lit(None))
            .otherwise(
                pl.struct(
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
                )
            )
        )
        return struct_vl.alias(meta.name) if meta else struct_vl
    elif isinstance(dtype, ArrayType):
        list_vl = (
            pl.when(base_expr.is_null())
            .then(pl.lit(None))
            .otherwise(
                base_expr.list.eval(
                    _get_expr(pl.element(), dtype=dtype.element_type, meta=meta)
                )
            )
        )
        return list_vl.alias(meta.name) if meta else list_vl
    return base_expr.alias(meta.name) if meta else base_expr


def _try_get_type(dtype: "DataType") -> "pl.DataType":
    import polars as pl

    if not isinstance(dtype, PrimitiveType):
        return None

    dtype_str = str(dtype.type)
    if dtype_str == "string":
        return pl.String
    elif dtype_str == "int":
        return pl.Int32
    elif dtype_str == "long":
        return pl.Int64
    elif dtype_str == "double":
        return pl.Float64
    elif dtype_str == "float":
        return pl.Float32
    elif dtype_str == "boolean":
        return pl.Boolean
    elif dtype_str == "date":
        return pl.Date32
    elif dtype_str == "timestamp":
        return pl.DateTime
    elif dtype_str == "binary":
        return pl.Binary
    elif dtype_str == "decimal":
        return pl.Decimal
    elif dtype_str == "short":
        return pl.Int16
    elif dtype == "byte":
        return pl.Int8
    elif dtype_str == "null":
        return pl.Null
    return None


def scan_delta_union(delta_table: DeltaTable | Path) -> "pl.LazyFrame":
    import polars as pl
    from .protocol_check import check_is_supported

    if isinstance(delta_table, Path):
        delta_table = DeltaTable(delta_table)
    check_is_supported(delta_table)
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
            pl_dtype = _try_get_type(field.type)
            pn = field.metadata.get("delta.columnMapping.physicalName", field.name)
            if "partition_values" in ac and pn in ac["partition_values"]:
                part_vl = ac["partition_values"][pn]
                selects.append(pl.lit(part_vl, pl_dtype).alias(field.name))
            elif "partition." + pn in ac:
                part_vl = ac["partition." + pn]
                selects.append(pl.lit(part_vl, pl_dtype).alias(field.name))
            elif "partition_values" in ac and field.name in ac["partition_values"]:
                # as of delta 0.14
                part_vl = ac["partition_values"][field.name]
                selects.append(pl.lit(part_vl, pl_dtype).alias(field.name))
            elif "partition." + field.name in ac:
                # as of delta 0.14
                part_vl = ac["partition." + field.name]
                selects.append(pl.lit(part_vl, pl_dtype).alias(field.name))
            elif (
                field.metadata.get("delta.columnMapping.physicalName", field.name)
                in parquet_schema
            ):
                selects.append(_get_expr(None, field.type, field))

        ds = base_ds.select(*selects)
        all_ds.append(ds)
    return pl.concat(all_ds, how="diagonal_relaxed")
