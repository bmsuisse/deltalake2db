import pytest
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture()
def storage_options():
    return {"use_emulator": "True"}


@pytest.fixture(scope="session", autouse=True)
def spawn_azurite():
    import test_server
    import os

    if os.getenv("NO_AZURITE_DOCKER", "0") == "1":
        yield None
    else:
        azurite = test_server.start_azurite()
        yield azurite
        if (
            os.getenv("KEEP_AZURITE_DOCKER", "0") == "0"
        ):  # can be handy during development
            azurite.stop()
