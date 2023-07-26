import collections
import io
import socket
import struct
from typing import Any, Tuple, Union

from five_one_one_kv.c import (
    RES_BAD_ARGS,
    RES_BAD_CMD,
    RES_BAD_HASH,
    RES_BAD_IX,
    RES_BAD_KEY,
    RES_BAD_OP,
    RES_BAD_TYPE,
    RES_ERR_CLIENT,
    RES_ERR_SERVER,
    RES_OK,
    RES_UNKNOWN,
    dumps,
    dumps_hashable,
    loads,
)
from five_one_one_kv.exceptions import (
    ClientError,
    EmbeddedCollectionError,
    NotEnoughDataError,
    NotHashableError,
    ServerError,
)

PIPE_UNSTARTED = 0
PIPE_CONTEXT_MODE = 1
PIPE_NONCONTEXT_MODE = 2
PIPE_FINISHED = 3


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
    try:
        return struct.pack("".join(struct_fmt_list), total_len, *struct_args)
    except struct.error:
        raise ClientError


def _unpack(res: bytes) -> Tuple[int, bytes]:
    """
    Wrapper for unpacking response.
    """
    if len(res) < _DOUBLEOFFSET:
        raise NotEnoughDataError
    # unpack expected response len
    try:
        (msg_len,) = struct.unpack_from("=i", res)
    except struct.error:
        raise ServerError
    if msg_len <= 0:
        raise ServerError
    # unpack response status
    try:
        (status,) = struct.unpack_from("=i", res, offset=_SINGLEOFFSET)
    except struct.error:
        raise ServerError
    # unpack data
    data_len = msg_len - _SINGLEOFFSET
    if data_len <= 0:
        return status, None
    if len(res) - _DOUBLEOFFSET < data_len:
        raise NotEnoughDataError
    try:
        (data,) = struct.unpack_from(f"={data_len}s", res, offset=_DOUBLEOFFSET)
    except struct.error:
        raise ServerError
    return status, data


def _unpack_from(res: bytes, offset=0) -> Tuple[int, bytes, int]:
    """
    Wrapper for unpacking response.
    """
    if len(res) - offset < _DOUBLEOFFSET:
        raise NotEnoughDataError
    # unpack expected response len
    try:
        (msg_len,) = struct.unpack_from("=i", res, offset=offset)
    except struct.error:
        raise ServerError
    if msg_len <= 0:
        raise ServerError
    offset += _SINGLEOFFSET
    # unpack response status
    try:
        (status,) = struct.unpack_from("=i", res, offset=offset)
    except struct.error:
        raise ServerError
    offset += _SINGLEOFFSET
    # unpack data
    data_len = msg_len - _SINGLEOFFSET
    if data_len <= 0:
        return status, None, offset
    if len(res) - offset < data_len:
        raise NotEnoughDataError
    res_format = f"={data_len}s"
    try:
        (data,) = struct.unpack_from(res_format, res, offset=offset)
    except struct.error:
        raise ServerError
    offset += struct.calcsize(res_format)
    return status, data, offset


_code_to_exc = collections.defaultdict(
    lambda: Exception("Encountered unrecognized status")
)
_code_to_exc[RES_UNKNOWN] = Exception(
    "Something went wrong but the server does not know what"
)
_code_to_exc[RES_ERR_SERVER] = ServerError("Server encountered an error")
_code_to_exc[RES_ERR_CLIENT] = ClientError("Server claims client made a mistake")
_code_to_exc[RES_BAD_CMD] = Exception("Server did not recognize the command")
_code_to_exc[RES_BAD_TYPE] = TypeError("Server did not recognize the argument type")
_code_to_exc[RES_BAD_KEY] = KeyError("")
_code_to_exc[RES_BAD_ARGS] = TypeError("Server did not recognize the argument format")
_code_to_exc[RES_BAD_OP] = AttributeError(
    "The type associated with the key does not support the command"
)
_code_to_exc[RES_BAD_IX] = IndexError("")
_code_to_exc[RES_BAD_HASH] = TypeError("Tried to send an unhashable type as a key")


class Client:
    def __init__(self):
        self._sock = socket.create_connection(("0.0.0.0", 8513))

    def get(self, key: Any) -> bytes:
        key = dumps_hashable(key)
        self._sock.send(_pack(b"get", key))
        status, data = self._looped_recv()
        if status == RES_OK:
            return loads(data)
        if status == RES_BAD_KEY:
            return None
        raise _code_to_exc[status]

    def __getitem__(self, key: Any) -> Any:
        key = dumps_hashable(key)
        self._sock.send(_pack(b"get", key))
        status, data = self._looped_recv()
        if status == RES_OK:
            return loads(data)
        if status == RES_BAD_KEY:
            raise KeyError("key %s not found in server" % (key,))
        raise _code_to_exc[status]

    def __setitem__(self, key: Any, val: Any) -> None:
        key = dumps_hashable(key)
        val = dumps(val)
        self._sock.send(_pack(b"put", key, val))
        status, data = self._looped_recv()
        if status == RES_OK:
            return
        raise _code_to_exc[status]

    def __delitem__(self, key: Any) -> None:
        key = dumps_hashable(key)
        self._sock.send(_pack(b"del", key))
        status, data = self._looped_recv()
        if status == RES_OK:
            return
        raise _code_to_exc[status]

    def _looped_recv(self):
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


class Pipeline:
    def __init__(self):
        self._sock = socket.create_connection(("0.0.0.0", 8513))
        self._keys = []
        self._wbuff = []

    def __enter__(self):
        return self

    def __exit__(self):
        self._exec()

    def submit(self):
        res = self._exec()
        return res

    def _exec(self):
        self._sock.send(b"".join(self._wbuff))
        return self._looped_recv()

    def get(self, key: Any) -> bytes:
        key = dumps_hashable(key)
        self._keys.append(key)
        self._wbuff.append(_pack(b"get", key))

    def __setitem__(self, key: Any, val: Any) -> None:
        key = dumps_hashable(key)
        self._keys.append(key)
        val = dumps(val)
        self._wbuff.append(_pack(b"put", key, val))

    def __delitem__(self, key: Any) -> None:
        key = dumps_hashable(key)
        self._keys.append(key)
        self._wbuff.append(_pack(b"del", key))

    def _looped_recv(self):
        response = b""
        status = RES_UNKNOWN
        data = b""
        offset = 0
        results = []
        while len(self._wbuff) > len(results):
            response += self._sock.recv(1024)
            try:
                while True:
                    status, data, offset = _unpack_from(response, offset=offset)
                    if status == RES_OK:
                        if data is None:
                            results.append(None)
                            continue
                        results.append(loads(data))
                        continue
                    if status == RES_BAD_KEY:
                        k = self._keys[len(results)]
                        results.append(KeyError("key %s not found in server" % (k,)))
                        continue
                    results.append(_code_to_exc[status])
            except NotEnoughDataError:
                continue
        return results
