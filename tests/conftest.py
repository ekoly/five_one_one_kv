import logging
import logging.handlers
import sys
import time

import pytest

from five_one_one_kv import Client, Pipeline

logger = logging.getLogger()
logger.handlers.clear()
formatter = logging.Formatter("%(asctime)s | %(name)s | %(levelname)s | %(message)s")
sh = logging.StreamHandler(sys.stdout)
sh.setFormatter(formatter)
logger.addHandler(sh)
wfh = logging.handlers.WatchedFileHandler("server.log")
wfh.setFormatter(formatter)
logger.addHandler(wfh)
logger.setLevel(logging.DEBUG)


@pytest.fixture(scope="function")
def client():
    client = Client()
    yield client
    client.close()


@pytest.fixture(scope="function")
def pipeline():
    client = Pipeline()
    yield client
    client.close()
