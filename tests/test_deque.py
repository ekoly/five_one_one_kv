import collections

import pytest

from five_one_one_kv.exceptions import EmbeddedCollectionError

from .utils import randostrs


def test_basic(client):
    key = randostrs()
    control = collections.deque()
    client.queue(key, 10)
    for _ in range(32):
        val = randostrs()
        control.append(val)
        client.push(key, val)

    for _ in range(32):
        assert control.popleft() == client.pop(key)

    with pytest.raises(IndexError):
        client.pop(key)


def test_embed_collections(client):
    key = randostrs()
    client.queue(key, 10)
    with pytest.raises(EmbeddedCollectionError):
        client.push(key, [1, 1, 2, 3, 5])
