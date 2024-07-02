from typing import Union, Optional, Callable, TYPE_CHECKING
from pathlib import Path
from deltalake import DeltaTable

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


def get_delta_table(
    uri_or_table: Union[str, Path, DeltaTable],
    storage_options: Optional[dict] = None,
    *,
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]" = None,
) -> DeltaTable:
    base_path = (
        uri_or_table.table_uri
        if isinstance(uri_or_table, DeltaTable)
        else (
            uri_or_table
            if isinstance(uri_or_table, str)
            else str(uri_or_table.absolute())
        )
    )
    base_path = base_path.removesuffix("/")
    if isinstance(uri_or_table, Path) or isinstance(uri_or_table, str):
        from .azure_helper import get_storage_options_object_store

        path_for_delta, storage_options_for_delta = get_storage_options_object_store(
            uri_or_table, storage_options, get_credential
        )
        return DeltaTable(path_for_delta, storage_options=storage_options_for_delta)
    return uri_or_table
