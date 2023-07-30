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

(The server supports `-v` for minimal logging and `-vv` for verbose logging, but keep in mind
that the server.log file will grow in size quite rapidly with `-vv`)

With the server running, run the test suite with:
```
make test
```

## Features

The server supports a number of python types, each of which may have the "hashable" and/or "collectable"
attributes. "Hashable" types may be keys. "Collectable" types may be a member of a collection.

| Type | Is Hashable | Is Collectable |
| --- | --- | --- |
| int | yes | yes |
| float | yes | yes |
| str | yes | yes |
| bytes | yes | yes |
| list | no | no |
| bool | no | yes |
| datetime | no | yes |
| queue | no | no |

Bools are forbidden from being keys as a style choice.

Queues are a special case which can only be created with the "queue" command. Queues cannot be retrieved
with the "get" command but the queue can be manipulated with the "push" and "pop" commands.

Additionally the server supports TTL, which can be set by supplying a datetime argument to "set" commands.
TTL currently works to the nearest second and does not respect datetimes with microseconds.

The server protects users from simultaneously modifying storage with a C semaphore. I hope to add user locks
in the near future.

## Implementation

The server has at minimum 3 threads: a poll loop, and connection io loop, and a ttl loop.

The poll loop calls the C `poll` function on a 1-second timeout to find out which idle connections have
incoming requests. These connections are pushed onto a queue of my own implementation. Then a C pthread condition
is notified.

The connection io loop waits for the pthread condition and then pops a connection from the queue. It then enters
a state machine which reads from the connection and then tries to process the request. "Processing the request"
means:
 * read a 16-bit unsigned int of the total message size
 * read a 16-bit unsigned int of the number of strings in the request
 * read each string, which is composed of a 16 bit unsigned int of the string length, a char indicating the type, and a human-readable string representation of the data.

The state machine then executes the request which usually involves manipulating a Python dictionary storing the key/value
pairs. It then writes a response which is in the same format as above.

The ttl loop involves a TTL heap (my own heap implementation inspired by Python's heap module) and a pthread condition.
The condition will be notified when the TTL at the front of the heap has changed, and times out when the TTL at the front of
the heap has expired. When the condition is notified or has timed out, the loop checks if the TTL at the front of the heap
has expired. If it has expired, it is popped from the heap and the corresponding key is deleted from the storage
dictionary. Then the loop resets the condition based on the next TTL and continues.

The client is significantly simpler. It uses Python's `struct` module to create bytestrings that have the format of C structs.
These bytestrings are written and read from a socket. The client uses the same C-implemented methods as the server to serialize
and deserialize data.
