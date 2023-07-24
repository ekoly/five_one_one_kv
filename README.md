# five-one-one-kv

This is a toy KV server project inspired by build-your-own.org's Redis course.

The server is written in C/CPython with a Python wrapper.

The client is written in Python.

Install with:
```
make install
```

Run the server with:
```
python -m five_one_one_kv.server
```

With the server running, run the test suite with:
```
make test
```
