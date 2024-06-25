from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional, Union
from deltalake import DataType, Field
from deltalake.schema import StructType, ArrayType, PrimitiveType, MapType
import sqlglot.expressions as ex
from deltalake2db.azure_helper import (
    get_storage_options_object_store,
)
from deltalake2db.filter_by_meta import _can_filter
import deltalake2db.sql_utils as squ

if TYPE_CHECKING:
    from deltalake import DeltaTable
    from azure.core.credentials import TokenCredential
    import duckdb


def _cast(s: ex.Expression, t: Optional[ex.DATA_TYPE]):
    if t is None:
        return s
    return ex.cast(s, t)


def _dummy_expr(
    field_type: "PrimitiveType | StructType | ArrayType | MapType",
) -> ex.Expression:
    from deltalake.schema import PrimitiveType

    if isinstance(field_type, PrimitiveType):
        if str(field_type).startswith("decimal("):
            cast_as = ex.DataType.build(str(field_type))
        else:
            cast_as = type_map.get(field_type.type)
        return _cast(ex.Null(), cast_as)
    elif isinstance(field_type, StructType):
        return squ.struct(
            {
                subfield.name: _dummy_expr(subfield.type)
                for subfield in field_type.fields
            }
        )
    elif isinstance(field_type, ArrayType):
        return ex.Array(expressions=[_dummy_expr(field_type.element_type)])
    elif isinstance(field_type, MapType):
        return ex.MapFromEntries(
            this=ex.Array(
                expressions=[
                    ex.Tuple(
                        expressions=[
                            _dummy_expr(field_type.key_type),
                            _dummy_expr(field_type.value_type),
                        ]
                    )
                ]
            )
        )
    raise ValueError(f"Unsupported type {field_type}")


def _get_expr(
    base_expr: "ex.Expression|None",
    dtype: "DataType",
    meta: Optional[Field],
    alias=True,
    *,
    counter: int = 0,
) -> "ex.Expression":
    pn = (
        meta.metadata.get("delta.columnMapping.physicalName", meta.name)
        if meta
        else None
    )
    if base_expr is None:
        assert pn is not None
        base_expr = ex.Column(this=pn)
    if isinstance(dtype, StructType):
        struct_expr = (
            ex.case()
            .when(base_expr.is_(ex.Null()), ex.Null())
            .else_(
                squ.struct(
                    {
                        subfield.name: _get_expr(
                            ex.Dot(
                                this=base_expr,
                                expression=ex.Identifier(
                                    this=subfield.metadata.get(
                                        "delta.columnMapping.physicalName",
                                        subfield.name,
                                    ),
                                    quoted=True,
                                ),
                            ),
                            subfield.type,
                            subfield,
                            alias=False,
                            counter=counter + 1,
                        )
                        for subfield in dtype.fields
                    }
                )
            )
        )
        if alias and meta is not None:
            return struct_expr.as_(meta.name)
        return struct_expr
    elif isinstance(dtype, ArrayType):
        vl = squ.list_transform(
            "x_" + str(counter),
            base_expr,
            _get_expr(
                ex.Identifier(this="x_" + str(counter), quoted=False),
                dtype.element_type,
                meta=meta,
                alias=False,
                counter=counter + 1,
            ),
        )
        if alias and meta is not None:
            return vl.as_(meta.name)
        return vl

    return base_expr.as_(meta.name) if meta is not None and alias else base_expr


def load_install_azure(con: "duckdb.DuckDBPyConnection"):
    with con.cursor() as cur:
        cur.execute(
            "select loaded, installed from duckdb_extensions() where extension_name='azure' "
        )
        res = cur.fetchone()
        loaded = res[0] if res else False
        installed = res[0] if res else False
    if not installed:
        con.install_extension("azure")
    if not loaded:
        con.load_extension("azure")


def apply_storage_options(
    con: "duckdb.DuckDBPyConnection",
    storage_options: dict,
    *,
    type="azure",
    account_name_path: Optional[str] = None,
):
    if type in ["az", "abfs", "abfss"]:
        type = "azure"
    if type != "azure":
        raise ValueError("Only azure is supported for now")
    load_install_azure(con)
    with con.cursor() as cur:
        cur.execute("FROM duckdb_secrets() where type='azure';")
        secrets = cur.fetchall()
        se_names = [s[0] for s in secrets]
    if len(se_names) > 0:  # for now, only one secret is supported in DuckDB, it seems
        return
    account_name = storage_options.get(
        "account_name",
        storage_options.get("azure_storage_account_name", account_name_path),
    )
    use_emulator = storage_options.get("use_emulator", "0") in ["1", "True", "true"]
    secret_name = account_name or ("_emulator" if use_emulator else "")

    if secret_name not in se_names:
        if "connection_string" in storage_options:
            cr = storage_options["connection_string"]
            con.execute(
                f"""CREATE SECRET {secret_name} (
TYPE AZURE,
CONNECTION_STRING '{cr}'
);"""
            )
        elif account_name is not None and "account_key" in storage_options:
            ak = storage_options["account_key"]
            conn_str = f"AccountName={account_name};AccountKey={ak};BlobEndpoint=https://{account_name}.blob.core.windows.net;"
            con.execute(
                f"""CREATE SECRET {secret_name} (
TYPE AZURE,
CONNECTION_STRING '{conn_str}',
ACCOUNT_NAME '{account_name}'
);"""
            )
        elif account_name is not None and "sas_token" in storage_options:
            sas_token = storage_options["sas_token"]
            conn_str = f"BlobEndpoint=https://{account_name}.blob.core.windows.net;SharedAccessSignature={sas_token}"
            con.execute(
                f"""CREATE SECRET {secret_name} (
TYPE AZURE,
CONNECTION_STRING '{conn_str}',
ACCOUNT_NAME '{account_name}'
);"""
            )
        elif account_name is not None:
            chain = storage_options.get("chain", "default")
            con.execute(
                f"""CREATE SECRET {secret_name} (
TYPE AZURE,
PROVIDER CREDENTIAL_CHAIN,
CHAIN '{chain}',
ACCOUNT_NAME '{account_name}'
);"""
            )
        elif use_emulator:
            con.execute(
                f"""CREATE SECRET {secret_name} (
TYPE AZURE,
CONNECTION_STRING 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;',
ACCOUNT_NAME 'devstoreaccount1'
);"""
            )
        else:
            raise ValueError(
                "No connection string or account_name and account_key provided"
            )


type_map = {
    "byte": ex.DataType.Type.UTINYINT,
    "int": ex.DataType.Type.INT,
    "integer": ex.DataType.Type.INT,
    "long": ex.DataType.Type.BIGINT,
    "boolean": ex.DataType.Type.BOOLEAN,
    "date": ex.DataType.Type.DATE,
    "timestamp": ex.DataType.Type.TIMESTAMPTZ,
    "float": ex.DataType.Type.FLOAT,
    "double": ex.DataType.Type.DOUBLE,
    "string": ex.DataType.Type.VARCHAR,
    "short": ex.DataType.Type.SMALLINT,
    "binary": ex.DataType.Type.BINARY,
    "timestampNtz": ex.DataType.Type.TIMESTAMP,
    "timestamp_ntz": ex.DataType.Type.TIMESTAMP,
    "decimal": ex.DataType.Type.DECIMAL,
}


def create_view_for_delta(
    con: "duckdb.DuckDBPyConnection",
    delta_table: "Union[DeltaTable , Path , str]",
    view_name: str,
    overwrite=True,
    *,
    conditions: Optional[dict] = None,
    storage_options: Optional[dict] = None,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
):
    sql = get_sql_for_delta(
        delta_table,
        duck_con=con,
        conditions=conditions,
        storage_options=storage_options,
        get_credential=get_credential,
    )
    assert '"' not in view_name
    if overwrite:
        con.execute(f'create or replace view "{view_name}" as {sql}')
    else:
        con.execute(f'create view "{view_name}" as {sql}')


def get_sql_for_delta_expr(
    dt: "Union[DeltaTable,Path , str]",
    conditions: Union[Optional[dict], Sequence[ex.Expression], ex.Expression] = None,
    select: Union[Sequence[Union[str, ex.Expression]], None] = None,
    distinct=False,
    cte_wrap_name: Union[str, None] = None,
    action_filter: Union[Callable[[dict], bool], None] = None,
    sql_prefix="delta",
    delta_table_cte_name: Union[str, None] = None,
    duck_con: "Union[duckdb.DuckDBPyConnection, None]" = None,
    storage_options: Optional[dict] = None,
    *,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
) -> ex.Select:
    from .sql_utils import read_parquet, union, filter_via_dict

    account_name_path = None
    if isinstance(dt, Path) or isinstance(dt, str):
        from deltalake import DeltaTable

        path_for_delta, storage_options_for_delta = get_storage_options_object_store(
            dt, storage_options, get_credential
        )
        account_name_path = (
            storage_options_for_delta.get("account_name", None)
            if storage_options_for_delta
            else None
        )
        dt = DeltaTable(path_for_delta, storage_options=storage_options_for_delta)

    from .protocol_check import check_is_supported

    check_is_supported(dt)

    delta_table_cte_name = delta_table_cte_name or sql_prefix + "_delta_table"
    from deltalake.schema import PrimitiveType

    file_selects: list[ex.Select] = []

    delta_fields = dt.schema().fields
    owns_con = False
    if duck_con is None:
        import duckdb

        duck_con = duckdb.connect()
        owns_con = True
    if dt.table_uri.startswith("az://") or dt.table_uri.startswith("abfss://"):
        apply_storage_options(
            duck_con,
            storage_options or dt._storage_options or {},
            type="azure",
            account_name_path=account_name_path,
        )  # type: ignore

    try:
        for ac in dt.get_add_actions(flatten=True).to_pylist():
            if (
                conditions is not None
                and isinstance(conditions, dict)
                and _can_filter(ac, conditions)
            ):
                continue
            if action_filter and not action_filter(ac):
                continue
            fullpath = dt.table_uri.removesuffix("/") + "/" + ac["path"]
            with duck_con.cursor() as cur:
                cur.execute(f"select name from parquet_schema('{fullpath}')")
                cols: list[str] = [c[0] for c in cur.fetchall()]
            cols_sql: list[ex.Expression] = []
            for field in delta_fields:
                field_name = field.name
                phys_name = field.metadata.get(
                    "delta.columnMapping.physicalName", field_name
                )

                cast_as = None

                if isinstance(field.type, PrimitiveType):
                    if str(field.type).startswith("decimal("):
                        cast_as = ex.DataType.build(str(field.type))
                    else:
                        cast_as = type_map.get(field.type.type)
                if "partition_values" in ac and phys_name in ac["partition_values"]:
                    cols_sql.append(
                        _cast(
                            ex.convert(ac["partition_values"][phys_name]), cast_as
                        ).as_(field_name)
                    )
                elif "partition." + phys_name in ac:
                    cols_sql.append(
                        _cast(ex.convert(ac["partition." + phys_name]), cast_as).as_(
                            field_name
                        )
                    )
                elif "partition_values" in ac and field.name in ac["partition_values"]:
                    cols_sql.append(
                        _cast(
                            ex.convert(ac["partition_values"][field.name]), cast_as
                        ).as_(field_name)
                    )
                elif "partition." + field.name in ac:
                    cols_sql.append(
                        _cast(ex.convert(ac["partition." + field.name]), cast_as).as_(
                            field_name
                        )
                    )
                elif phys_name in cols:
                    cols_sql.append(
                        _get_expr(ex.column(phys_name, quoted=True), field.type, field)
                    )
                else:
                    cols_sql.append(ex.Null().as_(field_name))

            select_pq = ex.select(
                *cols_sql
            ).from_(
                read_parquet(ex.convert(fullpath))
            )  # "SELECT " + ", ".join(cols_sql) + " FROM read_parquet('" + fullpath + "')"
            file_selects.append(select_pq)
        if len(file_selects) == 0:
            file_selects = []
            fields = [_dummy_expr(field.type).as_(field.name) for field in delta_fields]
            file_selects.append(ex.select(*fields).where("1=0"))
        file_sql = ex.CTE(
            this=union(file_selects, distinct=False), alias=delta_table_cte_name
        )
        if select:
            select_exprs = [
                ex.column(s, quoted=True) if isinstance(s, str) else s for s in select
            ]
        else:
            select_exprs = [ex.Star()]

        conds = (
            filter_via_dict(conditions) if isinstance(conditions, dict) else conditions
        )

        se = ex.select(*select_exprs)
        if distinct:
            se = se.distinct()
        if conds is not None:
            se = se.where(*conds)
        se = se.from_(delta_table_cte_name)

        if cte_wrap_name:
            s = ex.select(ex.Star()).from_(cte_wrap_name)
            # s = s.with_()
            s.with_(file_sql.alias, file_sql.args["this"], copy=False)
            s.with_(cte_wrap_name, se, copy=False)
            return s
        else:
            se.with_(file_sql.alias, file_sql.args["this"], copy=False)
            return se
    finally:
        if owns_con:
            duck_con.close()


def get_sql_for_delta(
    dt: "Union[DeltaTable, Path, str]",
    conditions: Optional[dict] = None,
    select: Union[list[str], None] = None,
    distinct=False,
    action_filter: Union[Callable[[dict], bool], None] = None,
    cte_wrap_name: Union[str, None] = None,
    sql_prefix="delta",
    duck_con: "Union[duckdb.DuckDBPyConnection, None]" = None,
    storage_options: Optional[dict] = None,
    *,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
) -> str:
    expr = get_sql_for_delta_expr(
        dt=dt,
        conditions=conditions,
        select=select,
        distinct=distinct,
        action_filter=action_filter,
        sql_prefix=sql_prefix,
        cte_wrap_name=cte_wrap_name,
        duck_con=duck_con,
        storage_options=storage_options,
        get_credential=get_credential,
    )
    if cte_wrap_name:
        suffix_sql = ex.select(ex.Star()).from_(cte_wrap_name).sql(dialect="duckdb")
        ws = expr.sql(dialect="duckdb")
        without_suffix = ws.removesuffix(suffix_sql)
        assert without_suffix != ws
        return without_suffix
    return expr.sql(dialect="duckdb")
