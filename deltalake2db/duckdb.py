from collections.abc import Sequence
import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable
from deltalake import DataType, Field
from deltalake.schema import StructType, ArrayType
import sqlglot.expressions as ex
import deltalake2db.sql_utils as squ

if TYPE_CHECKING:
    from deltalake import DeltaTable
    import duckdb


def _cast(s: ex.Expression, t: ex.DataType.Type | None):
    if t is None:
        return s
    return ex.cast(s, t)


def _get_expr(
    base_expr: "ex.Expression|None",
    dtype: "DataType",
    meta: Field | None,
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
    secrect_name = storage_options.get(
        "account_name",
        storage_options.get(
            "azure_storage_account_name",
            "_emulator" if storage_options.get("use_emulator", "0") == "True" else "",
        ),
    )

    if secrect_name not in se_names:
        if "connection_string" in storage_options:
            cr = storage_options["connection_string"]
            con.execute(
                f"""CREATE SECRET {secrect_name} (
TYPE AZURE,
CONNECTION_STRING '{cr}'
);"""
            )
        elif "account_name" in storage_options and "account_key" in storage_options:
            an = storage_options["account_name"]
            ak = storage_options["account_key"]
            conn_str = f"AccountName={an};AccountKey={ak};BlobEndpoint=https://{an}.blob.core.windows.net;"
            con.execute(
                f"""CREATE SECRET {secrect_name} (
TYPE AZURE,
CONNECTION_STRING '{conn_str}'
);"""
            )
        elif "account_name" in storage_options:
            an = storage_options["account_name"]
            con.execute(
                f"""CREATE SECRET {secrect_name} (
TYPE AZURE,
PROVIDER CREDENTIAL_CHAIN,
ACCOUNT_NAME '{an}'
);"""
            )
        elif storage_options.get("use_emulator", None):
            con.execute(
                f"""CREATE SECRET {secrect_name} (
TYPE AZURE,
CONNECTION_STRING 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;'
);"""
            )
        else:
            raise ValueError(
                "No connection string or account_name and account_key provided"
            )


type_map = {
    "byte": ex.DataType.Type.UTINYINT,
    "int": ex.DataType.Type.INT,
    "long": ex.DataType.Type.BIGINT,
    "boolean": ex.DataType.Type.BOOLEAN,
    "date": ex.DataType.Type.DATE,
    "timestamp": ex.DataType.Type.TIMESTAMP,
    "float": ex.DataType.Type.FLOAT,
    "double": ex.DataType.Type.DOUBLE,
}


def get_sql_for_delta_expr(
    dt: "DeltaTable | Path",
    conditions: dict | None | Sequence[ex.Expression] | ex.Expression = None,
    select: Sequence[str | ex.Expression] | None = None,
    distinct=False,
    cte_wrap_name: str | None = None,
    action_filter: Callable[[dict], bool] | None = None,
    sql_prefix="delta",
    delta_table_cte_name: str | None = None,
    duck_con: "duckdb.DuckDBPyConnection | None" = None,
) -> ex.Select | None:
    from .sql_utils import read_parquet, union, filter_via_dict

    if isinstance(dt, Path):
        from deltalake import DeltaTable

        dt = DeltaTable(dt)
    from .protocol_check import check_is_supported

    check_is_supported(dt)

    delta_table_cte_name = delta_table_cte_name or sql_prefix + "_delta_table"
    dt.update_incremental()
    from deltalake.schema import PrimitiveType

    file_selects: list[ex.Select] = []

    delta_fields = dt.schema().fields
    owns_con = False
    if duck_con is None:
        import duckdb

        duck_con = duckdb.connect()
        owns_con = True
    if dt.table_uri.startswith("az://"):
        apply_storage_options(duck_con, dt._storage_options, type="azure")  # type: ignore

    try:

        for ac in dt.get_add_actions(flatten=True).to_pylist():
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

            select_pq = ex.select(*cols_sql).from_(
                read_parquet(ex.convert(fullpath))
            )  # "SELECT " + ", ".join(cols_sql) + " FROM read_parquet('" + fullpath + "')"
            file_selects.append(select_pq)
        if len(file_selects) == 0:
            return None
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
    dt: "DeltaTable | Path",
    conditions: dict | None = None,
    select: list[str] | None = None,
    distinct=False,
    action_filter: Callable[[dict], bool] | None = None,
    cte_wrap_name: str | None = None,
    sql_prefix="delta",
    duck_con: "duckdb.DuckDBPyConnection | None" = None,
) -> str | None:
    expr = get_sql_for_delta_expr(
        dt=dt,
        conditions=conditions,
        select=select,
        distinct=distinct,
        action_filter=action_filter,
        sql_prefix=sql_prefix,
        cte_wrap_name=cte_wrap_name,
        duck_con=duck_con,
    )
    if expr is None:
        return None
    if cte_wrap_name:
        suffix_sql = ex.select(ex.Star()).from_(cte_wrap_name).sql(dialect="duckdb")
        ws = expr.sql(dialect="duckdb")
        without_suffix = ws.removesuffix(suffix_sql)
        assert without_suffix != ws
        return without_suffix
    return expr.sql(dialect="duckdb")
