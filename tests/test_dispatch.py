import datetime

import pytest

from five_one_one_kv.c import dumps, dumps_hashable, loads, loads_hashable
from five_one_one_kv.exceptions import EmbeddedCollectionError, NotHashableError


def test_bad_load_type():
    x = None
    with pytest.raises(TypeError):
        loads(x)


def test_int():
    x = 511
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_float():
    x = 3.14159
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_str():
    x = "Glücksburg"
    dumped_x = dumps(x)
    try:
        loaded_x = loads(dumped_x)
    except ValueError:
        print(dumped_x)
    assert loaded_x == x


def test_bytes():
    x = "mel ott"
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_list():
    x = [511, "Glücksburg", b"mel ott", 3.14159]
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_bool_true():
    x = True
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_bool_false():
    x = False
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_list_embedded():
    x = [
        511,
        [
            "Glücksburg",
        ],
        b"mel ott",
        3.14159,
    ]
    with pytest.raises(EmbeddedCollectionError):
        dumps(x)


def test_list_hashable():
    x = [1, 2, 3]
    with pytest.raises(NotHashableError):
        dumps_hashable(x)


def test_datetime():
    x = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=0)))
    x = x.replace(microsecond=0)
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_datetime_hashable():
    x = datetime.datetime.now(tz=datetime.timezone(datetime.timedelta(hours=0)))
    x = x.replace(microsecond=0)
    with pytest.raises(NotHashableError):
        dumps_hashable(x)


def test_dumps_invalid_args():
    with pytest.raises(TypeError):
        dumps(1, 2, 3)


def test_loads_invalid_args():
    with pytest.raises(TypeError):
        loads(1, 2, 3)


def test_tuple():
    x = (1, 2, 3)
    dumped_x = dumps(x)
    loaded_x = loads(dumped_x)
    assert loaded_x == x


def test_hashable_tuple():
    x = (1, 2, 3)
    dumped_x = dumps_hashable(x)
    loaded_x = loads_hashable(dumped_x)
    assert loaded_x == x


def test_tuple_embedded_collection():
    x = (1, [3, 5, 8], 3)
    with pytest.raises(EmbeddedCollectionError):
        dumps(x)


def test_invalid_hashable_tuple():
    x = (1, 2, 3, datetime.datetime.now())
    with pytest.raises(NotHashableError):
        dumps_hashable(x)
