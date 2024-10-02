from pathlib import Path
from typing import TYPE_CHECKING, cast, Union, Optional, Callable

from deltalake2db.azure_helper import get_storage_options_object_store
from deltalake2db.filter_by_meta import _can_filter

if TYPE_CHECKING:
    import polars as pl
    from azure.core.credentials import TokenCredential
from deltalake import DeltaTable, Field, DataType
from deltalake.schema import StructType, ArrayType, MapType
from collections import OrderedDict
import os


def _get_expr(
    base_expr: "Union[pl.Expr,None]",
    dtype: "DataType",
    meta: Optional[Field],
    parquet_field: "Optional[pl.PolarsDataType]",
) -> "pl.Expr":
    import polars as pl

    if parquet_field is None:
        return (
            pl.lit(None, _get_type(dtype, False)).alias(meta.name)
            if meta
            else pl.lit(None, _get_type(dtype, False))
        )

    pn = (
        meta.metadata.get("delta.columnMapping.physicalName", meta.name)
        if meta
        else None
    )
    if base_expr is None:
        assert pn is not None
        base_expr = pl.col(pn)
    if isinstance(dtype, StructType):

        def _get_sub_type(name: str) -> "pl.PolarsDataType| None":
            if parquet_field is None:
                return None
            assert isinstance(parquet_field, pl.Struct)
            for f in parquet_field.fields:
                if f.name == name:
                    return f.dtype
            return None

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
                            _get_sub_type(
                                subfield.metadata.get(
                                    "delta.columnMapping.physicalName", subfield.name
                                )
                            ),
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
                    _get_expr(
                        pl.element(),
                        dtype=dtype.element_type,
                        meta=meta,
                        parquet_field=cast(pl.List, parquet_field).inner,
                    )
                )
            )
        )
        return list_vl.alias(meta.name) if meta else list_vl
    return base_expr.alias(meta.name) if meta else base_expr


def _get_type(dtype: "DataType", physical: bool) -> "pl.PolarsDataType":
    import polars as pl

    if isinstance(dtype, StructType):
        return pl.Struct(
            [
                pl.Field(
                    f.name
                    if not physical
                    else f.metadata.get("delta.columnMapping.physicalName", f.name),
                    _get_type(f.type, physical),
                )
                for f in dtype.fields
            ]
        )
    if isinstance(dtype, ArrayType):
        return pl.List(_get_type(dtype.element_type, physical))
    if isinstance(dtype, MapType):
        raise NotImplementedError("MapType not supported in polars")

    dtype_str = str(dtype.type)
    if dtype_str == "string":
        return pl.String
    elif dtype_str in ["int", "integer"]:
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
        return pl.Date
    elif dtype_str == "timestamp":
        return pl.Datetime
    elif dtype_str == "binary":
        return pl.Binary
    elif dtype_str.startswith("decimal"):
        precision, scale = dtype_str.split("(")[1].split(")")[0].split(",")
        return pl.Decimal(int(precision), int(scale))
    elif dtype_str == "short":
        return pl.Int16
    elif dtype == "byte":
        return pl.Int8
    elif dtype_str == "null":
        return pl.Null
    elif dtype_str in ["timestampNtz", "timestamp_ntz"]:
        return pl.Datetime
    raise NotImplementedError(f"{dtype_str} not supported in polars currently")


def _filter_cond(f: "pl.LazyFrame", conditions: dict) -> "pl.LazyFrame":
    return f.filter(**conditions)


def get_polars_schema(
    delta_table: Union[DeltaTable, Path, str],
    physical_name: bool = False,
) -> "OrderedDict[str, pl.PolarsDataType]":
    from .protocol_check import check_is_supported

    if isinstance(delta_table, Path) or isinstance(delta_table, str):
        delta_table = DeltaTable(delta_table)
    check_is_supported(delta_table)
    res_dict = OrderedDict()
    for f in delta_table.schema().fields:
        pn = f.name
        if physical_name:
            pn = f.metadata.get("delta.columnMapping.physicalName", f.name)
        res_dict[pn] = _get_type(f.type, physical_name)
    return res_dict


def scan_delta_union(
    delta_table: Union[DeltaTable, Path, str],
    conditions: Optional[dict] = None,
    storage_options: Optional[dict] = None,
    *,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
) -> "pl.LazyFrame":
    import polars as pl
    from .protocol_check import check_is_supported

    if isinstance(delta_table, Path) or isinstance(delta_table, str):
        path_for_delta, storage_options_for_delta = get_storage_options_object_store(
            delta_table, storage_options, get_credential
        )
        delta_table = DeltaTable(
            path_for_delta, storage_options=storage_options_for_delta
        )
    check_is_supported(delta_table)
    all_ds = []
    all_fields = delta_table.schema().fields
    physical_schema = get_polars_schema(delta_table, physical_name=True)
    physical_schema_no_parts = physical_schema.copy()
    for pc in delta_table.metadata().partition_columns:
        physical_schema_no_parts.pop(pc)
    for ac in delta_table.get_add_actions(flatten=True).to_pylist():
        if conditions is not None and _can_filter(ac, conditions):
            continue
        fullpath = os.path.join(delta_table.table_uri, ac["path"]).replace("\\", "/")
        base_ds = pl.scan_parquet(
            fullpath,
            storage_options=delta_table._storage_options,
            glob=False,
            schema=physical_schema_no_parts,
            allow_missing_columns=True,
        )
        selects = []
        for field in all_fields:
            pn = field.metadata.get("delta.columnMapping.physicalName", field.name)
            if "partition_values" in ac and pn in ac["partition_values"]:
                part_vl = ac["partition_values"][pn]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False)).alias(field.name)
                )
            elif "partition." + pn in ac:
                part_vl = ac["partition." + pn]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False)).alias(field.name)
                )
            elif "partition_values" in ac and field.name in ac["partition_values"]:
                # as of delta 0.14
                part_vl = ac["partition_values"][field.name]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False)).alias(field.name)
                )
            elif "partition." + field.name in ac:
                # as of delta 0.14
                part_vl = ac["partition." + field.name]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False)).alias(field.name)
                )
            else:
                selects.append(
                    _get_expr(
                        None,
                        field.type,
                        field,
                        physical_schema.get(
                            field.metadata.get(
                                "delta.columnMapping.physicalName", field.name
                            )
                        ),
                    )
                )

        ds = (
            _filter_cond(base_ds.select(*selects), conditions)
            if conditions is not None
            else base_ds.select(*selects)
        )
        all_ds.append(ds)
    if len(all_ds) == 0:
        return pl.DataFrame(
            data=[], schema={f.name: _get_type(f.type, False) for f in all_fields}
        ).lazy()
    return pl.concat(all_ds, how="diagonal_relaxed")
