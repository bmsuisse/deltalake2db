from pathlib import Path
import docker
from docker.models.containers import Container
from time import sleep
from typing import Optional, cast
import docker.errors
import os


def get_test_blobstorage():
    constr = os.getenv(
        "TEST_BLOB_CONSTR",
        "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;",
    )
    from azure.storage.blob import ContainerClient

    cc = ContainerClient.from_connection_string(constr, "testlakedb")
    if not cc.exists():
        cc.create_container()
    else:
        cc.delete_container()
        cc.create_container()
    return cc


def upload_to_azurite():
    from azure.core.exceptions import ResourceExistsError

    with get_test_blobstorage() as cc:
        fakeroot = Path("tests/data/faker2")
        for root, _, fls in os.walk(fakeroot):
            for fl in fls:
                try:
                    rel = str(Path(root).relative_to(fakeroot))
                    with open(os.path.join(root, fl), "rb") as f:
                        target_path = (
                            f"td/delta/fake/{rel}/{fl}"
                            if rel != "."
                            else f"td/delta/fake/{fl}"
                        )
                        cc.upload_blob(target_path, f)
                except ResourceExistsError:
                    pass  # already uploaded


def start_azurite() -> Container:
    client = docker.from_env()  # code taken from https://github.com/fsspec/adlfs/blob/main/adlfs/tests/conftest.py#L72
    azurite_server: Optional[Container] = None
    try:
        m = cast(Container, client.containers.get("test4azurite"))
        if m.status == "running":
            upload_to_azurite()
            return m
        else:
            azurite_server = m
    except docker.errors.NotFound:
        pass

    if azurite_server is None:
        azurite_server = client.containers.run(
            "mcr.microsoft.com/azure-storage/azurite:latest",
            detach=True,
            name="test4azurite",
            ports={"10000/tcp": 10000, "10001/tcp": 10001, "10002/tcp": 10002},
        )  # type: ignore
    assert azurite_server is not None
    azurite_server.start()
    print(azurite_server.status)
    sleep(20)
    upload_to_azurite()
    print("Successfully created azurite container...")
    return azurite_server
