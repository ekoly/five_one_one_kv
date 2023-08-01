import concurrent.futures
import datetime
import random
import time

import pytest

from five_one_one_kv.client import Client

from .utils import randostrs


def test_ttl_seconds(client):
    client.set("veni", "vidi", 2)
    assert client["veni"] == "vidi"
    time.sleep(3)
    with pytest.raises(KeyError):
        client["veni"]


def test_ttl_timedelta(client):
    client.set("a", 1, datetime.timedelta(seconds=2))
    assert client["a"] == 1
    time.sleep(3)
    with pytest.raises(KeyError):
        client["a"]


def test_ttl_datetime(client):
    dt = datetime.datetime.now()
    dt += datetime.timedelta(seconds=2)
    client.set("b", 2, dt)
    assert client["b"] == 2
    time.sleep(3)
    with pytest.raises(KeyError):
        client["a"]


def test_ttl_many():
    def _challenge():
        client = Client()
        keys = [randostrs() for _ in range(10)]
        values = [randostrs() for _ in range(10)]
        ttls = [random.randint(3, 7) for _ in range(10)]
        for k, v, ttl in zip(keys, values, ttls):
            client.set(k, v, ttl)
        for k, v in zip(keys, values):
            if client.get(k) != v:
                client.close()
                return (False, "first check failed")
        time.sleep(max(ttls) + 1)
        for k in keys:
            if client.get(k):
                client.close()
                return (False, "second check failed")
        client.close()
        return True, ""

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        for _ in range(64):
            futures.append(executor.submit(_challenge))
        for fut in concurrent.futures.as_completed(futures):
            if fut.exception():
                raise fut.exception()
            is_success, msg = fut.result()
            if not is_success:
                raise AssertionError(msg)


def test_ttl_many_extend_ttl():
    """
    Test that ttls can be extended.
    """

    def _challenge():
        client = Client()
        keys = [randostrs() for _ in range(10)]
        values = [randostrs() for _ in range(10)]
        ttl_secs = 3
        for k, v in zip(keys, values):
            client.set(k, v, ttl_secs)
        # loop because we want to extend the ttls a few times over
        for ix in range(5):
            time.sleep(ttl_secs - 2)
            for k, v in zip(keys, values):
                if client.get(k) != v:
                    client.close()
                    return (False, f"check {ix} failed")
            for k in keys:
                client.ttl(k, ttl_secs)
        time.sleep(ttl_secs + 1)
        for k in keys:
            if client.get(k):
                client.close()
                return (False, "final check failed")
        client.close()
        return True, ""

    futures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        for _ in range(64):
            futures.append(executor.submit(_challenge))
        for fut in concurrent.futures.as_completed(futures):
            if fut.exception():
                raise fut.exception()
            is_success, msg = fut.result()
            if not is_success:
                raise AssertionError(msg)
