from typing import Optional, TYPE_CHECKING, Callable

if TYPE_CHECKING:
    from azure.core.credentials import TokenCredential


def apply_azure_chain(
    storage_options: Optional[dict],
    get_credential: "Optional[Callable[[str], Optional[TokenCredential]]]",
):
    # support for "Chain" as in duckdb: https://duckdb.org/docs/extensions/azure#credential_chain-provider
    if storage_options is None:
        return None
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
        return new_opts
    return storage_options
