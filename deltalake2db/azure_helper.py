from typing import Optional, TYPE_CHECKING, Callable, Union
from pathlib import Path

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


def get_storage_options_object_store(
    path: Union[Path, str],
    storage_options: Optional[dict],
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]",
):
    # support for "Chain" as in duckdb: https://duckdb.org/docs/extensions/azure#credential_chain-provider
    if storage_options is None:
        return path, None
    new_path = path
    if isinstance(new_path, str) and (
        ".blob.core.windows.net" in new_path or ".dfs.core.windows.net" in new_path
    ):
        from urllib.parse import urlparse

        up = urlparse(new_path)
        account_name_from_url = up.netloc.split(".")[0]

        new_path = new_path.replace(up.scheme + "://" + up.netloc, up.scheme + "://")
    else:
        account_name_from_url = None
    chain = storage_options.get("chain", None)
    get_credential = get_credential or (lambda x: None)

    def _get_cred(chain: str):
        if get_credential is None:
            return None

    if chain is not None:
        from azure.identity import (
            ChainedTokenCredential,
            DefaultAzureCredential,
            ManagedIdentityCredential,
            AzureCliCredential,
            EnvironmentCredential,
        )

        creds = chain.split(";")
        map = {
            "cli": AzureCliCredential,
            "env": EnvironmentCredential,
            "managed_identity": ManagedIdentityCredential,
            "default": DefaultAzureCredential,
        }
        creds = [get_credential(c) or map[c]() for c in creds]
        cred = ChainedTokenCredential(*creds) if len(creds) > 1 else creds[0]
        token = storage_options["token"] = cred.get_token(
            "https://storage.azure.com/.default"
        ).token
        new_opts = storage_options.copy()
        new_opts.pop("chain")
        new_opts["token"] = token
        if account_name_from_url:
            new_opts["account_name"] = new_opts.get(
                "account_name", account_name_from_url
            )
        return new_path, new_opts
    if account_name_from_url is not None and "account_name" not in storage_options:
        new_opts = storage_options.copy()
        new_opts["account_name"] = account_name_from_url
        return new_path, new_opts
    return new_path, storage_options
