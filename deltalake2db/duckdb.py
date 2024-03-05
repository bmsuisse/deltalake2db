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
        struct_expr = squ.struct(
            {
                subfield.name: _get_expr(
                    ex.Dot(
                        this=base_expr,
                        expression=ex.Identifier(
                            this=subfield.metadata.get(
                                "delta.columnMapping.physicalName", subfield.name
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
        if alias:
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
        if alias:
            return vl.as_(meta.name)
        return vl

    return base_expr.as_(meta.name) if meta and alias else base_expr


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
    try:

        for ac in dt.get_add_actions(flatten=True).to_pylist():
            if action_filter and not action_filter(ac):
                continue
            fullpath = os.path.join(dt.table_uri, ac["path"])
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
                if isinstance(field.type, PrimitiveType) and field.type.type == "byte":
                    cast_as = ex.DataType.Type.UTINYINT
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
                read_parquet(Path(fullpath))
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
