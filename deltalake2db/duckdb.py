from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Optional, Union, cast
import sqlglot.expressions as ex
from deltalake2db.azure_helper import (
    get_storage_options_fsspec,
    get_storage_options_object_store,
    get_account_name_from_path,
    AZURE_EMULATOR_CONNECTION_STRING,
)
from deltalake2db.filter_by_meta import _can_filter, _partition_value_to_python
import deltalake2db.sql_utils as squ
from deltalake2db.delta_meta_retrieval import (
    DataType,
    DuckDBEngine,
    PrimitiveType,
    get_meta,
    field_to_type,
    PrimitiveField,
    Field,
)

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential
    import duckdb


def _cast(s: ex.Expression, t: Optional[ex.DATA_TYPE]):
    if t is None:
        return s
    return ex.cast(s, t)


def _dummy_expr(
    field_type: "DataType | PrimitiveField",
) -> ex.Expression:
    if isinstance(field_type, str):
        if str(field_type).startswith("decimal("):
            cast_as = ex.DataType.build(str(field_type))
        else:
            cast_as = type_map.get(field_type)
        return _cast(ex.Null(), cast_as)
    elif field_type["type"] == "struct":
        return squ.struct(
            {
                subfield["name"]: _dummy_expr(subfield)
                for subfield in field_type["fields"]
            }
        )
    elif field_type["type"] == "array":
        return ex.Array(expressions=[_dummy_expr(field_type["elementType"])])
    elif field_type["type"] == "map":
        return ex.MapFromEntries(
            this=ex.Array(
                expressions=[
                    ex.Tuple(
                        expressions=[
                            _dummy_expr(field_type["keyType"]),
                            _dummy_expr(field_type["valueType"]),
                        ]
                    )
                ]
            )
        )
    else:
        field_type = field_type["type"]
        if str(field_type).startswith("decimal("):
            cast_as = ex.DataType.build(str(field_type))
        else:
            cast_as = type_map.get(field_type)
        return _cast(ex.Null(), cast_as)


def _get_expr(
    base_expr: "ex.Expression|None",
    dtype: "DataType",
    meta: "Optional[Field]",
    alias=True,
    *,
    counter: int = 0,
) -> "ex.Expression":
    pn = (
        meta.get("metadata", {}).get("delta.columnMapping.physicalName", meta["name"])
        if meta
        else None
    )
    if base_expr is None:
        assert pn is not None
        base_expr = ex.Column(this=pn)
    if isinstance(dtype, dict) and dtype["type"] == "struct":
        struct_expr = (
            ex.case()
            .when(base_expr.is_(ex.Null()), ex.Null())
            .else_(
                squ.struct(
                    {
                        subfield["name"]: _get_expr(
                            ex.Dot(
                                this=base_expr,
                                expression=ex.Identifier(
                                    this=subfield.get("metadata", {}).get(
                                        "delta.columnMapping.physicalName",
                                        subfield["name"],
                                    ),
                                    quoted=True,
                                ),
                            ),
                            field_to_type(subfield),
                            subfield,
                            alias=False,
                            counter=counter + 1,
                        )
                        for subfield in dtype["fields"]
                    }
                )
            )
        )
        if alias and meta is not None:
            return struct_expr.as_(meta["name"])
        return struct_expr
    elif isinstance(dtype, dict) and dtype["type"] == "array":
        vl = squ.list_transform(
            "x_" + str(counter),
            base_expr,
            _get_expr(
                ex.Identifier(this="x_" + str(counter), quoted=False),
                dtype["elementType"],
                meta=meta,
                alias=False,
                counter=counter + 1,
            ),
        )
        if alias and meta is not None:
            return vl.as_(meta["name"])
        return vl

    return base_expr.as_(meta["name"]) if meta is not None and alias else base_expr


def load_install_extension(con: "duckdb.DuckDBPyConnection", ext_name: str):
    with con.cursor() as cur:
        cur.execute(
            "select loaded, installed from duckdb_extensions() where extension_name=$ext_name ",
            {
                "ext_name": ext_name,
            },
        )
        res = cur.fetchone()
        loaded = res[0] if res else False
        installed = res[0] if res else False
    if not installed:
        con.install_extension(ext_name)
    if not loaded:
        con.load_extension(ext_name)


def apply_storage_options(
    con: "duckdb.DuckDBPyConnection",
    uri: Union[str, Path],
    storage_options: Optional[dict],
    *,
    use_fsspec: bool = False,
):
    if storage_options is None:
        return
    if isinstance(uri, str) and (
        ".blob.core.windows.net" in uri or ".dfs.core.windows.net" in uri
    ):
        from urllib.parse import urlparse

        up = urlparse(uri)
        account_name_from_url = up.netloc.split(".")[0]
    else:
        account_name_from_url = None
    if use_fsspec:
        apply_storage_options_fsspec(
            con,
            uri if isinstance(uri, str) else str(uri.absolute()),
            storage_options,
            account_name_from_url,
        )
    else:
        apply_storage_options_azure_ext(
            con,
            storage_options,
            account_name_path=account_name_from_url,
        )


def apply_storage_options_fsspec(
    con: "duckdb.DuckDBPyConnection",
    base_path: str,
    storage_options: dict,
    account_name_path: Optional[str] = None,
):
    use_emulator = storage_options.get("use_emulator", "0") in ["1", "True", "true"]

    opts = get_storage_options_fsspec(storage_options)
    account_name = storage_options.get(
        "account_name",
        storage_options.get("azure_storage_account_name", account_name_path),
    )
    if not use_emulator:
        assert account_name is not None, "account_name must be provided for fsspec"
    proto_name_duckdb = "fsspec_az_" + (account_name or "_emulator")
    if not con.filesystem_is_registered(proto_name_duckdb):
        from .fsspec_fs import get_az_blob_fs

        fs = get_az_blob_fs(proto_name_duckdb, **opts)

        con.register_filesystem(fs)  # type: ignore

    return proto_name_duckdb


def apply_storage_options_azure_ext(
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
    load_install_extension(con, "azure")
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
    if str(storage_options.get("anon", "0")).lower() in ["1", "true"]:
        return None  # no need to create a secret for anon (which is an fsspec-thing)
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
        elif account_name is not None and "client_id" in storage_options:
            con.execute(
                f"""CREATE SECRET {secret_name} (
TYPE AZURE,
PROVIDER SERVICE_PRINCIPAL,
TENANT_ID  '{storage_options.get("tenant_id")}',
CLIENT_ID   '{storage_options.get("client_id")}',
CLIENT_SECRET    '{storage_options.get("client_secret")}',
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
CONNECTION_STRING '{AZURE_EMULATOR_CONNECTION_STRING}',
ACCOUNT_NAME 'devstoreaccount1'
);"""
            )
        else:
            raise ValueError(
                "No connection string or account_name and account_key provided"
            )
    return secret_name


type_map = {
    "byte": ex.DataType.Type.TINYINT,
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


def _to_dict(pv):
    if isinstance(pv, list):
        return {k["key"]: k["value"] for k in pv}
    return pv


def create_view_for_delta(
    con: "duckdb.DuckDBPyConnection",
    delta_table: "Union[Path , str]",
    view_name: str,
    overwrite=True,
    *,
    conditions: Optional[dict] = None,
    storage_options: Optional[dict] = None,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
    use_fsspec: bool = False,
    use_delta_ext=False,
    select: "Optional[list[str]]" = None,
    version: "Optional[int]" = None,
):
    sql = get_sql_for_delta(
        delta_table,
        duck_con=con,
        conditions=conditions,
        storage_options=storage_options,
        get_credential=get_credential,
        use_fsspec=use_fsspec,
        use_delta_ext=use_delta_ext,
        select=select,
        version=version,
    )
    assert '"' not in view_name
    if overwrite:
        con.execute(f'create or replace view "{view_name}" as {sql}')
    else:
        con.execute(f'create view "{view_name}" as {sql}')


def get_sql_for_delta_expr(
    table_or_path: "Union[Path , str]",
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
    use_fsspec=False,
    use_delta_ext=False,
    version: "Optional[int]" = None,
) -> ex.Select:
    from .sql_utils import read_parquet, union, filter_via_dict

    if isinstance(table_or_path, str):
        base_path = table_or_path
    else:
        base_path = str(table_or_path.absolute())

    base_path = base_path.removesuffix("/")
    is_azure = base_path.startswith("az://") or base_path.startswith("abfss://")
    account_name_path = get_account_name_from_path(base_path) if is_azure else None

    conds = filter_via_dict(conditions) if isinstance(conditions, dict) else conditions
    owns_con = False
    if duck_con is None:
        import duckdb

        duck_con = duckdb.connect()
        owns_con = True
    if use_delta_ext:
        load_install_extension(duck_con, "delta")
    if use_fsspec:
        fake_protocol = apply_storage_options_fsspec(
            duck_con,
            base_path,
            storage_options or {},
            account_name_path=account_name_path,
        )
        base_path = fake_protocol + "://" + base_path.split("://")[1]
    elif is_azure:
        apply_storage_options_azure_ext(
            duck_con,
            storage_options or {},
            type="azure",
            account_name_path=account_name_path,
        )  # type: ignore

    try:
        if not use_delta_ext:
            from .protocol_check import check_is_supported

            meta_state = get_meta(DuckDBEngine(duck_con), base_path, version=version)
            check_is_supported(meta_state)
            delta_table_cte_name = delta_table_cte_name or sql_prefix + "_delta_table"

            file_selects: list[ex.Select] = []
            assert meta_state.schema is not None
            delta_fields = meta_state.schema["fields"]
            physicalTypeMap: dict[str, PrimitiveType] = {
                f.get("metadata", {}).get(
                    "delta.columnMapping.physicalName", f["name"]
                ): cast(PrimitiveType, f["type"])
                for f in delta_fields
            }
            for ac in meta_state.add_actions.values():
                if (
                    conditions is not None
                    and isinstance(conditions, dict)
                    and _can_filter(ac, conditions, physicalTypeMap)
                ):
                    continue
                if action_filter and not action_filter(ac):
                    continue
                fullpath = base_path + "/" + ac["path"]
                with duck_con.cursor() as cur:
                    cur.execute(f"select name from parquet_schema('{fullpath}')")
                    cols: list[str] = [c[0] for c in cur.fetchall()]
                cols_sql: list[ex.Expression] = []
                for field in delta_fields:
                    field_name = field["name"]
                    phys_name = field.get("metadata", {}).get(
                        "delta.columnMapping.physicalName", field_name
                    )

                    cast_as = None

                    if isinstance(field["type"], str):
                        if str(field["type"]).startswith("decimal("):
                            cast_as = ex.DataType.build(str(field["type"]))
                        else:
                            cast_as = type_map.get(field["type"])
                    pv = _to_dict(ac.get("partitionValues", {}))
                    if phys_name in pv:
                        part_vl = pv[phys_name]

                        _type = field_to_type(field)
                        if isinstance(_type, str):
                            part_vl = _partition_value_to_python(part_vl, _type)
                        cols_sql.append(
                            _cast(ex.convert(part_vl), cast_as).as_(field_name)
                        )
                    elif "partition." + phys_name in ac:
                        cols_sql.append(
                            _cast(
                                ex.convert(ac["partition." + phys_name]), cast_as
                            ).as_(field_name)
                        )
                    elif (
                        "partitionValues" in ac
                        and field["name"] in ac["partitionValues"]
                    ):
                        cols_sql.append(
                            _cast(
                                ex.convert(ac["partitionValues"][field["name"]]),
                                cast_as,
                            ).as_(field_name)
                        )
                    elif "partition." + field["name"] in ac:
                        cols_sql.append(
                            _cast(
                                ex.convert(ac["partition." + field["name"]]), cast_as
                            ).as_(field_name)
                        )
                    elif phys_name in cols:
                        cols_sql.append(
                            _get_expr(
                                ex.column(phys_name, quoted=True),
                                field_to_type(field),
                                field,
                            )
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
                fields = [
                    _dummy_expr(field).as_(field["name"]) for field in delta_fields
                ]
                file_selects.append(ex.select(*fields).where("1=0"))
            file_sql = ex.CTE(
                this=union(file_selects, distinct=False), alias=delta_table_cte_name
            )
            if select:
                select_exprs = [
                    ex.column(s, quoted=True) if isinstance(s, str) else s
                    for s in select
                ]
            else:
                select_exprs = [ex.Star()]

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
        else:
            assert version is None, "version is not supported with delta extension"
            if select:
                select_exprs = [
                    ex.column(s, quoted=True) if isinstance(s, str) else s
                    for s in select
                ]
            else:
                select_exprs = [ex.Star()]

            se = ex.select(*select_exprs).from_(
                ex.func("delta_scan", ex.convert(base_path))
            )
            if distinct:
                se = se.distinct()
            if conds is not None:
                se = se.where(*conds)
            return se

    finally:
        if owns_con:
            duck_con.close()


def get_sql_for_delta(
    dt: "Union[Path, str]",
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
    use_fsspec: bool = False,
    use_delta_ext=False,
    version: "Optional[int]" = None,
) -> str:
    expr = get_sql_for_delta_expr(
        table_or_path=dt,
        conditions=conditions,
        select=select,
        distinct=distinct,
        action_filter=action_filter,
        sql_prefix=sql_prefix,
        cte_wrap_name=cte_wrap_name,
        duck_con=duck_con,
        storage_options=storage_options,
        get_credential=get_credential,
        use_fsspec=use_fsspec,
        use_delta_ext=use_delta_ext,
        version=version,
    )
    if cte_wrap_name:
        suffix_sql = ex.select(ex.Star()).from_(cte_wrap_name).sql(dialect="duckdb")
        ws = expr.sql(dialect="duckdb")
        without_suffix = ws.removesuffix(suffix_sql)
        assert without_suffix != ws
        return without_suffix
    return expr.sql(dialect="duckdb")
