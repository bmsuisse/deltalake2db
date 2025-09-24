from pathlib import Path
from typing import Optional, Sequence, Any, TypeVar
import sqlglot.expressions as ex
from typing import Union

from deltalake2db.filter_by_meta import FilterType


def read_parquet(
    path: Union[str, Path, list[Path], list[str], ex.Expression, list[ex.Expression]],
) -> ex.Expression:
    if isinstance(path, list):
        return ex.func(
            "read_parquet",
            ex.array(
                *[
                    ex.Literal.string(str(p)) if isinstance(p, (str, Path)) else p
                    for p in path
                ]
            ),
            ex.EQ(
                this=ex.Column(this=ex.Identifier(this="union_by_name", quoted=False)),
                expression=ex.Boolean(this=True),
            ),
        )
    return ex.func(
        "read_parquet",
        ex.Literal.string(str(path)) if isinstance(path, (str, Path)) else path,
    )


def union(selects: Sequence[ex.Expression], *, distinct: bool) -> ex.Expression:
    if len(selects) == 0:
        raise ValueError("No selects to union")
    elif len(selects) == 1:
        return selects[0]
    elif len(selects) == 2:
        return ex.union(selects[0], selects[1], distinct=distinct)
    else:
        return ex.union(
            selects[0], union(selects[1:], distinct=distinct), distinct=distinct
        )


def get_filter_expr(conditions: Optional[FilterType]):
    if not conditions:
        return None
    result_expr = None
    for k, operator, v in conditions:
        if operator == "=":
            if v is None:
                expr = ex.column(k, quoted=True).is_(ex.Null())
            else:
                expr = ex.column(k, quoted=True).eq(ex.convert(v))
        elif operator == "<":
            expr = ex.column(k, quoted=True) < ex.convert(v)
        elif operator == "<=":
            expr = ex.column(k, quoted=True) <= ex.convert(v)
        elif operator == ">":
            expr = ex.column(k, quoted=True) > ex.convert(v)
        elif operator == ">=":
            expr = ex.column(k, quoted=True) >= ex.convert(v)
        elif operator == "in":
            expr = ex.column(k, quoted=True).isin(*[ex.convert(i) for i in v])
        elif operator == "not in":
            expr = ~ex.column(k, quoted=True).isin(*[ex.convert(i) for i in v])
        else:
            raise ValueError(f"Unsupported operator: {operator}")
        if result_expr is None:
            result_expr = expr
        else:
            result_expr = ex.and_(result_expr, expr)
    return result_expr


T = TypeVar("T", bound=ex.Query)


def merge_ctes(select: T, other_select: ex.Select) -> T:
    new_select = select.copy()
    for cte in other_select.ctes:
        new_select.with_(cte.alias, cte.this, copy=False)
    return new_select


def struct(items: dict[str, ex.Expression]) -> ex.Struct:
    return ex.Struct(
        expressions=[
            ex.PropertyEQ(this=ex.Identifier(this=k, quoted=True), expression=v)
            for k, v in items.items()
        ]
    )


def list_transform(
    param_names: Union[list[str], str],
    list_expr: ex.Expression,
    value_expr: ex.Expression,
) -> ex.Expression:
    if isinstance(param_names, str):
        param_names = [param_names]
    return ex.func(
        "list_transform",
        list_expr,
        ex.Lambda(
            this=value_expr,
            expressions=[ex.Identifier(this=p, quoted=False) for p in param_names],
        ),
    )
