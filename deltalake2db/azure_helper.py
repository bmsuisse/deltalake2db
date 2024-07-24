from typing import Optional, TYPE_CHECKING, Callable, Union
from pathlib import Path

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential

AZURE_EMULATOR_CONNECTION_STRING = "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;"
STORAGE_SCOPE = "https://storage.azure.com/.default"


def _get_credential_from_chain(
    chain: str, get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]"
):
    from azure.identity import (
        ChainedTokenCredential,
        DefaultAzureCredential,
        ManagedIdentityCredential,
        AzureCliCredential,
        EnvironmentCredential,
    )

    get_credential = get_credential or (lambda x: None)

    creds = chain.split(";")
    map = {
        "cli": AzureCliCredential,
        "env": EnvironmentCredential,
        "managed_identity": ManagedIdentityCredential,
        "default": DefaultAzureCredential,
    }
    creds = [get_credential(c) or map[c]() for c in creds]
    cred = ChainedTokenCredential(*creds) if len(creds) > 1 else creds[0]
    return cred


def get_storage_options_fsspec(storage_options: dict):
    use_emulator = storage_options.get("use_emulator", "0") in ["1", "True", "true"]
    if "connection_string" not in storage_options and use_emulator:
        return {"connection_string": AZURE_EMULATOR_CONNECTION_STRING}
    elif (
        "account_key" not in storage_options
        and "anon" not in storage_options
        and "sas_token" not in storage_options
        and "account_name" in storage_options
        and "chain" not in storage_options
    ):  # anon is true by default in fsspec which makes no sense mostly
        return {"anon": False} | storage_options
    if "chain" in storage_options:
        chain = storage_options["chain"]
        creds = chain.split(";")
        assert len(creds) == 1, "chain must be one credential for fsspec"
        cred = creds[0]
        storage_options2 = storage_options.copy()
        storage_options2.pop("chain", None)
        if cred == "default":
            return {"anon": False} | storage_options2
        map_flags = {
            "_1": "exclude_workload_identity_credential",
            "_2": "exclude_developer_cli_credential",
            "cli": "exclude_cli_credential",
            "env": "exclude_environment_credential",
            "managed_identity": "exclude_managed_identity_credential",
            "_3": "exclude_powershell_credential",
            "_4": "exclude_visual_studio_code_credential",
            "_5": "exclude_shared_token_cache_credential",
            "_6": "exclude_interactive_browser_credential",
        }
        for k, v in map_flags.items():
            storage_options2[v] = k != cred
        return storage_options2 | {"anon": False}
    return storage_options


def get_account_name_from_path(path: str):
    if ".blob.core.windows.net" in path or ".dfs.core.windows.net" in path:
        from urllib.parse import urlparse

        up = urlparse(path)
        return up.netloc.split(".")[0]
    return None


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

        new_path = new_path.replace(
            up.scheme + "://" + up.netloc, up.scheme + "://"
        ).replace(":///", "://")
    else:
        account_name_from_url = None
    chain = storage_options.get("chain", None)

    if "anon" in storage_options:
        anon_value = str(storage_options["anon"]).lower()  # anon is an fsspec-thing
        if anon_value in ["0", "false"] and chain is None:
            chain = "default"

    get_credential = get_credential or (lambda x: None)

    def _get_cred(chain: str):
        if get_credential is None:
            return None

    if chain is not None:
        new_opts = storage_options.copy()
        new_opts.pop("chain", None)
        cred = _get_credential_from_chain(chain, get_credential)
        new_opts["token"] = cred.get_token(STORAGE_SCOPE).token
        if account_name_from_url:
            new_opts["account_name"] = new_opts.get(
                "account_name", account_name_from_url
            )
        return new_path, new_opts
    elif "anon" in storage_options and str(storage_options["anon"]).lower() in [
        "1",
        "true",
    ]:
        storage_options = storage_options.copy()
        storage_options.pop("anon")
    if account_name_from_url is not None and "account_name" not in storage_options:
        new_opts = storage_options.copy()
        new_opts["account_name"] = account_name_from_url
        return new_path, new_opts
    return new_path, storage_options
