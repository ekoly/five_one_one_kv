import random

import pytest

from five_one_one_kv.exceptions import NotHashableError


def randobytes():
    return bytes(random.randint(ord("a"), ord("z")) for _ in range(8))


def test_basics(pipeline):
    pipeline["foo"] = b"bar"
    pipeline.get("foo")
    del pipeline["foo"]
    pipeline.get("foo")
    results = pipeline.submit()
    assert results[0] is None
    assert results[1] == b"bar"
    assert results[2] is None
    assert isinstance(results[3], KeyError)


def test_overwrite(pipeline):
    pipeline["foo"] = b"bar"
    pipeline.get("foo")
    pipeline["foo"] = b"baz"
    pipeline.get("foo")
    pipeline["foo"] = b"buffoon"
    pipeline.get("foo")
    del pipeline["foo"]
    pipeline.get("foo")
    results = pipeline.submit()
    assert results[0] == None
    assert results[1] == b"bar"
    assert results[2] == None
    assert results[3] == b"baz"
    assert results[4] == None
    assert results[5] == b"buffoon"
    assert results[6] == None
    assert isinstance(results[7], KeyError)


@pytest.mark.parametrize(
    ("k",),
    (
        ("mel ott homeruns",),
        (b"buffoon",),
        (1001,),
        (2.35813,),
    ),
)
@pytest.mark.parametrize(
    ("v",),
    (
        ("511",),
        (b"baz",),
        ("dalmations",),
        (21.34,),
        (55,),
    ),
)
def test_types(pipeline, k, v):
    pipeline[k] = v
    pipeline.get(k)
    del pipeline[k]
    pipeline.get(k)
    results = pipeline.submit()
    assert results[0] is None
    assert results[1] == v
    assert results[2] is None
    assert isinstance(results[3], KeyError)


@pytest.mark.parametrize(
    ("k",),
    (
        (
            [
                1,
                2,
                3,
            ],
        ),
        (True,),
        (False,),
    ),
)
def test_unhashable_types(pipeline, k):
    with pytest.raises(NotHashableError):
        pipeline[k] = "bar"
