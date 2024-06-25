from azure.core.credentials import TokenCredential, AccessToken
from datetime import datetime, timedelta


class FakeCredential(TokenCredential):
    def get_token(self, *args, **kwargs) -> AccessToken:
        return AccessToken(
            "fake_token", int((datetime.now() + timedelta(days=1)).timestamp())
        )


def test_get_storage_options_object_store():
    from deltalake2db.azure_helper import get_storage_options_object_store

    new_path, creds = get_storage_options_object_store(
        "az://accountnamethatslong.blob.core.windows.net/testlakedb/td/delta/fake",
        {"chain": "default"},
        get_credential=lambda x: FakeCredential(),
    )
    assert isinstance(new_path, str)
    assert creds is not None
    assert ".blob.core" not in new_path
    assert ".dfs.core" not in new_path
    assert new_path.startswith("az://testlakedb/td/delta/fake")
    assert creds["token"] == "fake_token"
    assert creds["account_name"] == "accountnamethatslong"
    new_path, creds = get_storage_options_object_store(
        "abfss://accountnamethatslong.dfs.core.windows.net/testlakedb/td/delta/fake",
        {"chain": "default"},
        get_credential=lambda x: FakeCredential(),
    )
    assert isinstance(new_path, str)
    assert creds is not None
    assert ".blob.core" not in new_path
    assert ".dfs.core" not in new_path
    assert new_path.startswith("abfss://testlakedb/td/delta/fake")
    assert creds["token"] == "fake_token"
    assert creds["account_name"] == "accountnamethatslong"
