import json
import random

import pytest

from five_one_one_kv.c import RES_BAD_COLLECTION, RES_BAD_TYPE, dumps
from five_one_one_kv.client import _pack, _unpack
from five_one_one_kv.exceptions import TooLargeError

from .utils import randostrs


@pytest.mark.skip(reason="list implementation was changed")
def test_not_allow_embed_collections(client):
    my_list = []
    for _ in range(20):
        if random.randint(0, 2) == 0:
            elem = []
            for _ in range(20):
                elem.append(randostrs())
        else:
            elem = randostrs()
        my_list.append(dumps(elem).decode("ascii"))
    data = _pack(b"put", b"'bazoon", b"[" + json.dumps(my_list).encode("ascii"))
    client._sock.send(data)
    status, response = _unpack(client._sock.recv(1024))
    assert status == RES_BAD_COLLECTION


def test_bad_type(client):
    data = _pack(b"put", b"bbbbb", b"cccccccc")
    client._sock.send(data)
    status, response = _unpack(client._sock.recv(1024))
    assert status == RES_BAD_TYPE


def test_msg_too_large(client):
    my_list = []
    for _ in range(6000):
        elem = randostrs(size=16)
        my_list.append(dumps(elem).decode("ascii"))
    with pytest.raises(TooLargeError):
        data = _pack(b"put", b"bbazoon", b"[" + json.dumps(my_list).encode("ascii"))
