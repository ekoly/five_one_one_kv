import pytest
import time

from five_one_one_kv import Client, Pipeline


@pytest.fixture(scope="function")
def client():
    for _ in range(5):
        try:
            return Client()
        except ConnectionRefusedError:
            time.sleep(1.5)
    raise ConnectionRefusedError



@pytest.fixture(scope="function")
def pipeline():
    for _ in range(5):
        try:
            return Pipeline()
        except ConnectionRefusedError:
            time.sleep(1.5)
    raise ConnectionRefusedError
