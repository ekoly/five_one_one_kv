import collections
import json
import socket
import struct
from typing import Any, Tuple, Union

from five_one_one_kv.c import RES_ERR, RES_NX, RES_OK, RES_UNKNOWN, dumps, loads


class ServerError(Exception):
    """
    Server messed up.
    """

    pass


class NotEnoughDataError(Exception):
    """
    Need to read more data before parsing.
    """

    pass


_SINGLEOFFSET = struct.calcsize("=i")
_DOUBLEOFFSET = struct.calcsize("=ii")


def _pack(*args) -> bytes:
    """
    Wrapper for creating C struct of args.
    """
    struct_fmt_list = ["=", "i"]
    struct_args = [len(args)]
    for arg in args:
        arg_len = len(arg)
        struct_fmt_list.append("i")
        struct_args.append(arg_len)
        struct_fmt_list.append(f"{arg_len}s")
        struct_args.append(arg)
    total_len = struct.calcsize("".join(struct_fmt_list))
    struct_fmt_list.insert(1, "i")
    return struct.pack("".join(struct_fmt_list), total_len, *struct_args)


def _unpack(res: bytes) -> Tuple[int, bytes]:
    """
    Wrapper for unpacking response.
    """
    if len(res) < _SINGLEOFFSET:
        raise NotEnoughDataError
    # unpack expected response len
    (response_len,) = struct.unpack_from("=i", res)
    if response_len <= 0:
        raise ServerError
    # unpack response status
    (status,) = struct.unpack_from("=i", res, offset=_SINGLEOFFSET)
    # unpack data
    response_len -= _SINGLEOFFSET
    if response_len <= 0:
        return status, None
    if len(res) - _DOUBLEOFFSET < response_len:
        raise NotEnoughDataError
    try:
        (data,) = struct.unpack_from(f"{response_len}s", res, offset=_DOUBLEOFFSET)
    except struct.error:
        raise ServerError
    return status, data


class Client:
    def __init__(self):
        self._sock = socket.create_connection(("0.0.0.0", 8513))

    def get(self, key: Any) -> bytes:
        key = dumps(key)
        self._sock.send(_pack(b"get", key))
        status, data = self._looped_recv()
        if status == RES_ERR:
            raise ServerError
        if status == RES_NX:
            return None
        return loads(data)

    def __getitem__(self, key: Any) -> Any:
        key = dumps(key)
        self._sock.send(_pack(b"get", key))
        status, data = self._looped_recv()
        if status == RES_ERR:
            raise ServerError
        if status == RES_NX:
            raise KeyError
        return loads(data)

    def __setitem__(self, key: Any, val: Any) -> None:
        key = dumps(key)
        val = dumps(val)
        self._sock.send(_pack(b"put", key, val))
        status, data = self._looped_recv()
        if status != 0:
            raise ServerError

    def __delitem__(self, key: Any) -> None:
        key = dumps(key)
        self._sock.send(_pack(b"del", key))
        status, data = self._looped_recv()
        if status == RES_ERR:
            raise ServerError
        if status == RES_NX:
            raise KeyError

    def _looped_recv():
        response = b""
        status = RES_UNKNOWN
        data = b""
        while True:
            response += self._sock.recv(1024)
            try:
                status, data = _unpack(response)
            except NotEnoughDataError:
                continue
            else:
                break
        return status, data
