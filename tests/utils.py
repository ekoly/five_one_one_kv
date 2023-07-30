import random


def randobytes(size=8):
    return bytes(random.randint(ord("a"), ord("z")) for _ in range(size))


def randostrs(size=8):
    return randobytes(size=size).decode("ascii")
