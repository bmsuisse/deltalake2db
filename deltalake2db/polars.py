from pathlib import Path
from typing import TYPE_CHECKING, cast, Union, Optional, Callable, overload

from deltalake2db.arrow_utils import to_pylist
from deltalake2db.azure_helper import (
    get_storage_options_object_store,
    get_storage_options_fsspec,
)
from deltalake2db.filter_by_meta import _can_filter

if TYPE_CHECKING:
    import polars as pl
    import polars.schema as pl_schema
    from azure.core.credentials import TokenCredential
from collections import OrderedDict
import os

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from deltalake.schema import DataType, Field


class PolarsSettings:
    use_pyarrow = False
    exclude_fields: Optional[list[str]] = None
    fields: Optional[list[str]] = None

    timestamp_ntz_type: "pl.Datetime"
    timestamp_type: "pl.Datetime"

    def __init__(
        self,
        *,
        timestamp_ntz_type: "Optional[pl.Datetime]" = None,
        timestamp_type: "Optional[pl.Datetime]" = None,
        exclude_fields: Optional[list[str]] = None,
        fields: Optional[list[str]] = None,
        use_pyarrow=False,
    ):
        import polars as pl

        self.timestamp_ntz_type = timestamp_ntz_type or pl.Datetime(
            time_unit="us", time_zone=None
        )
        self.timestamp_type = timestamp_type or pl.Datetime(
            time_unit="us", time_zone="utc"
        )
        self.exclude_fields = exclude_fields
        self.fields = fields
        self.use_pyarrow = use_pyarrow


def _get_expr(
    base_expr: "Union[pl.Expr,None]",
    dtype: "DataType",
    meta: "Optional[Field]",
    parquet_field: "Optional[pl.PolarsDataType]",
    settings: "PolarsSettings",
) -> "pl.Expr":
    import polars as pl
    from deltalake.schema import StructType, ArrayType, MapType

    if parquet_field is None:
        return (
            pl.lit(None, _get_type(dtype, False, settings)).alias(meta.name)
            if meta
            else pl.lit(None, _get_type(dtype, False, settings))
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
                            settings,
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
                        settings=settings,
                    )
                )
            )
        )
        return list_vl.alias(meta.name) if meta else list_vl
    return base_expr.alias(meta.name) if meta else base_expr


def _get_type(
    dtype: "DataType", physical: bool, settings: "PolarsSettings"
) -> "Union[pl.DataType, type[pl.DataType]]":
    import polars as pl
    from deltalake.schema import StructType, ArrayType, MapType

    if isinstance(dtype, StructType):
        return pl.Struct(
            [
                pl.Field(
                    f.name
                    if not physical
                    else f.metadata.get("delta.columnMapping.physicalName", f.name),
                    _get_type(f.type, physical, settings),
                )
                for f in dtype.fields
            ]
        )
    if isinstance(dtype, ArrayType):
        return pl.List(_get_type(dtype.element_type, physical, settings))
    if isinstance(dtype, MapType):
        return pl.List(
            pl.Struct(
                [
                    pl.Field(
                        "key", _get_type(dtype.key_type, physical, settings=settings)
                    ),
                    pl.Field(
                        "value", _get_type(dtype.key_type, physical, settings=settings)
                    ),
                ]
            )
        )  # Polars models maps as structs with "key" and "value" fields

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
        return settings.timestamp_type
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
        return settings.timestamp_ntz_type
    else:
        return pl.Object


def get_polars_schema(
    delta_table: "Union[DeltaTable, Path, str]",
    physical_name: bool = False,
    settings: "Optional[PolarsSettings]" = None,
) -> "pl.Schema":
    from .protocol_check import check_is_supported
    import polars as pl
    from deltalake import DeltaTable

    if settings is None:
        settings = PolarsSettings()

    if isinstance(delta_table, Path) or isinstance(delta_table, str):
        delta_table = DeltaTable(delta_table)
    check_is_supported(delta_table)
    res_dict = OrderedDict()
    for f in delta_table.schema().fields:
        pn = f.name
        if settings.exclude_fields and f.name in settings.exclude_fields:
            continue
        if settings.fields and f.name not in settings.fields:
            continue
        if physical_name:
            pn = f.metadata.get("delta.columnMapping.physicalName", f.name)
        res_dict[pn] = _get_type(f.type, physical_name, settings)
    return pl.Schema(res_dict)


def _has_datetime_field(dt: "pl.PolarsDataType") -> bool:
    import polars as pl

    if isinstance(dt, pl.Datetime) or dt == pl.Datetime:
        return True
    if isinstance(dt, pl.Struct):
        return any(_has_datetime_field(f.dtype) for f in dt.fields)
    if isinstance(dt, pl.List):
        return _has_datetime_field(dt.inner)
    if isinstance(dt, pl.Array):
        return _has_datetime_field(dt.inner)
    return False


def _cast_schema(ds: "pl.DataFrame", schema: "pl_schema.Schema") -> "pl.DataFrame":
    import polars as pl

    exprs = []
    for name, dtype in schema.items():
        if name not in ds.columns:
            exprs.append(pl.lit(None, dtype).cast(dtype).alias(name))
        else:
            exprs.append(pl.col(name).cast(dtype).alias(name))
    return ds.select(exprs)


def _versiontuple(v):
    return tuple(map(int, (v.split("."))))


@overload
def scan_delta_union(
    delta_table: "Union[DeltaTable, Path, str]",
    conditions: Optional[dict] = None,
    storage_options: Optional[dict] = None,
    *,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
) -> "pl.LazyFrame": ...


@overload
def scan_delta_union(
    delta_table: "Union[DeltaTable, Path, str]",
    conditions: Optional[dict] = None,
    storage_options: Optional[dict] = None,
    *,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
    settings: "Optional[PolarsSettings]" = None,
) -> "Union[pl.LazyFrame, pl.DataFrame]": ...


def scan_delta_union(
    delta_table: "Union[DeltaTable, Path, str]",
    conditions: Optional[dict] = None,
    storage_options: Optional[dict] = None,
    *,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
    settings: "Optional[PolarsSettings]" = None,
) -> "Union[pl.LazyFrame, pl.DataFrame]":
    import polars as pl
    import polars.datatypes as pldt
    from .protocol_check import check_is_supported
    from deltalake import DeltaTable

    if settings is None:
        settings = PolarsSettings()

    if isinstance(delta_table, Path) or isinstance(delta_table, str):
        path_for_delta, storage_options_for_delta = get_storage_options_object_store(
            delta_table, storage_options, get_credential
        )
        delta_table = DeltaTable(
            path_for_delta, storage_options=storage_options_for_delta
        )
    else:
        path_for_delta = None
    check_is_supported(delta_table)
    all_ds: Union[list[pl.LazyFrame], list[pl.DataFrame]] = []
    all_fields = delta_table.schema().fields
    physical_schema = get_polars_schema(
        delta_table, physical_name=True, settings=settings
    )
    physical_schema_no_parts = physical_schema.copy()

    logical_to_physical = {
        f.name: f.metadata.get("delta.columnMapping.physicalName", f.name)
        for f in all_fields
        if (settings.exclude_fields is None or f.name not in settings.exclude_fields)
        and (settings.fields is None or f.name in settings.fields)
    }
    for pc in delta_table.metadata().partition_columns:
        physical_schema_no_parts.pop(logical_to_physical.get(pc, pc))

    datetime_fields = {
        key: value
        for key, value in physical_schema_no_parts.items()
        if _has_datetime_field(value)
    }

    dt_mapping: Optional[dict[str, Union[pl.DataType, pldt.DataTypeClass]]] = None
    dt_mapping_complete = False
    if (
        settings.use_pyarrow
        and storage_options
        and path_for_delta
        and "://" in str(path_for_delta)
    ):
        import pyarrow as pa
        import pyarrow.fs as pafs
        import fsspec

        proto = str(path_for_delta).split("://")[0]
        if proto == "az":
            proto = "abfss"
        storage_options_for_fsspec = get_storage_options_fsspec(storage_options)
        fs = fsspec.filesystem(proto, **storage_options_for_fsspec)
        pyarrow_opts = {"filesystem": fs}  # type: ignore
    else:
        pyarrow_opts = None
        storage_options_for_fsspec = None

    for ac in to_pylist(delta_table.get_add_actions(flatten=True)):
        if conditions is not None and _can_filter(ac, conditions):
            continue
        fullpath = os.path.join(delta_table.table_uri, ac["path"]).replace("\\", "/")
        if settings.use_pyarrow:
            ds = pl.read_parquet(
                fullpath,
                storage_options=storage_options_for_fsspec,
                glob=False,
                use_pyarrow=True,
                pyarrow_options=pyarrow_opts,
            )
        else:
            ds = None

        if datetime_fields and not dt_mapping_complete:
            # we need this as long as https://github.com/pola-rs/polars/issues/19533 is not done
            file_schema = (
                pl.scan_parquet(
                    fullpath,
                    storage_options=delta_table._storage_options,
                    glob=False,
                ).collect_schema()
                if ds is None
                else ds.schema
            )
            dt_mapping_complete = True
            for pn in datetime_fields.keys():
                dt_mapping = {}

                pys_type = file_schema.get(pn)
                if pys_type is None:
                    dt_mapping_complete = False
                    continue
                if isinstance(pys_type, pl.Int64):  # int64 is microsecond
                    dt_mapping[pn] = pl.Datetime(time_unit="us", time_zone="utc")
                else:
                    dt_mapping[pn] = pys_type

                physical_schema_no_parts[pn] = dt_mapping[pn]

        base_ds = (
            pl.scan_parquet(
                fullpath,
                storage_options=delta_table._storage_options,
                glob=False,
                schema=physical_schema_no_parts,
                missing_columns="insert",
                extra_columns="ignore",
            )
            if _versiontuple(pl.__version__) >= (1, 31)
            else pl.scan_parquet(
                fullpath,
                storage_options=delta_table._storage_options,
                glob=False,
                schema=physical_schema_no_parts,
                allow_missing_columns=True,
            )
            if ds is None
            else _cast_schema(ds, physical_schema_no_parts)
        )
        selects = []
        for field in all_fields:
            if settings.exclude_fields and field.name in settings.exclude_fields:
                continue
            if settings.fields and field.name not in settings.fields:
                continue
            pn = field.metadata.get("delta.columnMapping.physicalName", field.name)
            if "partition_values" in ac and pn in ac["partition_values"]:
                part_vl = ac["partition_values"][pn]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False, settings)).alias(
                        field.name
                    )
                )
            elif "partition." + pn in ac:
                part_vl = ac["partition." + pn]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False, settings)).alias(
                        field.name
                    )
                )
            elif "partition_values" in ac and field.name in ac["partition_values"]:
                # as of delta 0.14
                part_vl = ac["partition_values"][field.name]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False, settings)).alias(
                        field.name
                    )
                )
            elif "partition." + field.name in ac:
                # as of delta 0.14
                part_vl = ac["partition." + field.name]
                selects.append(
                    pl.lit(part_vl, _get_type(field.type, False, settings)).alias(
                        field.name
                    )
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
                        settings,
                    )
                )

        ds = (
            base_ds.select(*selects).filter(**conditions)
            if conditions is not None
            else base_ds.select(*selects)
        )
        all_ds.append(ds)  # type: ignore
    if len(all_ds) == 0:
        return pl.DataFrame(
            data=[],
            schema={f.name: _get_type(f.type, False, settings) for f in all_fields},
        ).lazy()
    return pl.concat(all_ds, how="diagonal_relaxed")
