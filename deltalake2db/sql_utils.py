from pathlib import Path
from typing import Optional, Sequence, Any, TypeVar
import sqlglot.expressions as ex
from typing import Union


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


def filter_via_dict(conditions: Optional[dict[str, Any]]):
    if not conditions or len(conditions) == 0:
        return None
    return [
        (
            ex.EQ(this=ex.column(k, quoted=True), expression=ex.convert(v))
            if v is not None
            else ex.Is(this=ex.column(k, quoted=True), expression=ex.Null())
        )
        for k, v in conditions.items()
    ]


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
