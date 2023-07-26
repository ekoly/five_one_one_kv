import pytest

from five_one_one_kv.c import dumps, dumps_hashable, loads, loads_hashable
from five_one_one_kv.exceptions import EmbeddedCollectionError, NotHashableError


def test_int():
    x = 511
    d = dumps(x)
    l = loads(d)
    assert l == x


def test_float():
    x = 3.14159
    d = dumps(x)
    l = loads(d)
    assert l == x


def test_str():
    x = "Glücksburg"
    d = dumps(x)
    try:
        l = loads(d)
    except ValueError:
        print(d)
    assert l == x


def test_bytes():
    x = "mel ott"
    d = dumps(x)
    l = loads(d)
    assert l == x


def test_list():
    x = [511, "Glücksburg", b"mel ott", 3.14159]
    d = dumps(x)
    l = loads(d)
    assert l == x


def test_bool_true():
    x = True
    d = dumps(x)
    l = loads(d)
    assert l == x


def test_bool_true():
    x = False
    d = dumps(x)
    l = loads(d)
    assert l == x


def test_list_imbedded():
    x = [
        511,
        [
            "Glücksburg",
        ],
        b"mel ott",
        3.14159,
    ]
    with pytest.raises(EmbeddedCollectionError):
        d = dumps(x)


def test_list_hashable():
    x = [1, 2, 3]
    with pytest.raises(NotHashableError):
        d = dumps_hashable(x)
