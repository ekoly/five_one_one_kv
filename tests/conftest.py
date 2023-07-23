import pytest
from five_one_one_kv import Client


@pytest.fixture(scope="session")
def client():
    return Client()
