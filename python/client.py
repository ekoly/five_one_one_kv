import collections
import logging
import socket
import struct
from datetime import datetime, timedelta, timezone
from typing import Any, Tuple, Union

from five_one_one_kv.c import (
    MAX_MSG_SIZE,
    RES_BAD_ARGS,
    RES_BAD_CMD,
    RES_BAD_COLLECTION,
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
    TooLargeError,
)

logger = logging.getLogger(__name__)


PIPE_UNSTARTED = 0
PIPE_CONTEXT_MODE = 1
PIPE_NONCONTEXT_MODE = 2
PIPE_FINISHED = 3


_SINGLEOFFSET = struct.calcsize("=H")
_DOUBLEOFFSET = struct.calcsize("=HH")


def _pack(*args) -> bytes:
    """
    Wrapper for creating C struct of args.
    """
    struct_fmt_list = ["=", "H", "H"]
    struct_args = [len(args)]
    for arg in args:
        arg_len = len(arg)
        struct_fmt_list.append("H")
        struct_args.append(arg_len)
        struct_fmt_list.append(f"{arg_len}s")
        struct_args.append(arg)
    fmt = "".join(struct_fmt_list)
    total_len = struct.calcsize(fmt) - _SINGLEOFFSET
    try:
        return struct.pack(fmt, total_len, *struct_args)
    except struct.error:
        raise TooLargeError("Tried to send a message to the server that was too large.")


def _unpack(res: bytes) -> Tuple[int, bytes]:
    """
    Wrapper for unpacking response.
    """
    if len(res) < _DOUBLEOFFSET:
        raise NotEnoughDataError
    # unpack expected response len
    try:
        (msg_len,) = struct.unpack_from("=H", res)
    except struct.error:
        raise ServerError
    if msg_len <= 0:
        raise ServerError
    # unpack response status
    try:
        (status,) = struct.unpack_from("=H", res, offset=_SINGLEOFFSET)
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
        (msg_len,) = struct.unpack_from("=H", res, offset=offset)
    except struct.error:
        raise ServerError
    if msg_len <= 0:
        raise ServerError
    offset += _SINGLEOFFSET
    # unpack response status
    try:
        (status,) = struct.unpack_from("=H", res, offset=offset)
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


def _convert_ttl(ttl: Any) -> bytes:
    dt_ttl = None
    if isinstance(ttl, int):
        dt_ttl = datetime.now(tz=timezone.utc)
        dt_ttl += timedelta(seconds=ttl)
    elif isinstance(ttl, timedelta):
        dt_ttl = datetime.now(tz=timezone.utc)
        dt_ttl += ttl
    elif isinstance(ttl, datetime):
        if ttl.tzinfo is None:
            dt_ttl = ttl.astimezone()
        else:
            dt_ttl = ttl
    else:
        raise TypeError("ttl argument must be datetime, timedelta, or int if given")
    ttl = dumps(dt_ttl)
    return ttl


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
_code_to_exc[RES_BAD_COLLECTION] = EmbeddedCollectionError(
    "Cannot embed a collection in another collection."
)


class Client:
    def __init__(self):
        self._sock = socket.create_connection(("0.0.0.0", 8513))

    def close(self):
        self._sock.shutdown(socket.SHUT_RDWR)
        self._sock.close()

    def _submit(self, key: Any, data: bytes, suppress_errors: tuple = None):
        if len(data) > MAX_MSG_SIZE:
            raise TooLargeError("Message size was too large.")
        self._sock.send(data)
        status, data = self._looped_recv()
        if status == RES_OK:
            if data:
                return loads(data)
            return None
        if suppress_errors:
            try:
                raise _code_to_exc[status]
            except suppress_errors:
                return None
        raise _code_to_exc[status]

    def get(self, key: Any) -> bytes:
        key = dumps_hashable(key)
        return self._submit(key, _pack(b"get", key), suppress_errors=(KeyError,))

    def __getitem__(self, key: Any) -> Any:
        key = dumps_hashable(key)
        return self._submit(key, _pack(b"get", key))

    def __setitem__(self, key: Any, val: Any) -> None:
        """
        Sends a request to the server to set `key` to `val`, potentially
        overwriting existing data at `key`.
        """
        key = dumps_hashable(key)
        val = dumps(val)
        return self._submit(key, _pack(b"put", key, val))

    def set(
        self, key: Any, val: Any, ttl: Union[datetime, timedelta, int, None] = None
    ) -> None:
        """
        Sends a request to the server to set `key` to `val`, potentially
        overwriting existing data at `key`.

        Args:
            key: the key to be set.
            val: the value to be set.
            ttl (optional): If given, the server will automatically delete
                the value after a time. If a `datetime` is given, it will be
                deleted at this time.  If a `timedelta` is given, it will be
                deleted after this amount of time. If an int is given, it will
                be deleted after this many seconds.
        """
        key = dumps_hashable(key)
        val = dumps(val)
        if ttl is not None:
            ttl = _convert_ttl(ttl)
            return self._submit(key, _pack(b"put", key, val, ttl))
        return self._submit(key, _pack(b"put", key, val))

    def __delitem__(self, key: Any) -> None:
        key = dumps_hashable(key)
        self._submit(key, _pack(b"del", key))

    def queue(
        self, key: Any, ttl: Union[datetime, timedelta, int, None] = None
    ) -> None:
        key = dumps_hashable(key)
        if ttl is not None:
            ttl = _convert_ttl(ttl)
            return self._submit(key, _pack(b"queue", key, ttl))
        return self._submit(key, _pack(b"queue", key))

    def push(self, key: Any, val: Any) -> None:
        key = dumps_hashable(key)
        val = dumps(val)
        return self._submit(key, _pack(b"push", key, val))

    def pop(self, key: Any) -> Any:
        key = dumps_hashable(key)
        return self._submit(key, _pack(b"pop", key))

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


class Pipeline(Client):
    def __init__(self):
        self._sock = socket.create_connection(("0.0.0.0", 8513))
        self._keys = []
        self._wbuff = []

    def _submit(self, key: Any, data: bytes, suppress_errors: tuple = tuple()) -> None:
        if len(data) > MAX_MSG_SIZE:
            raise TooLargeError("Message size was too large.")
        self._keys.append(key)
        self._wbuff.append(data)

    def execute(self):
        res = self._exec()
        return res

    def _exec(self):
        self._sock.send(b"".join(self._wbuff))
        return self._looped_recv()

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
