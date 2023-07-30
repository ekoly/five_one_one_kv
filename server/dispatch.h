#ifndef _FOO_DISPATCH_H
#define _FOO_DISPATCH_H

#include <stdint.h>
#include <Python.h>

#include "util.h"
#include "connection.h"
#include "pythontypes.h"

// the following directives are the hash of our commands
#define CMD_GET 118508615
#define CMD_PUT 612676312
#define CMD_DEL 1534775668
#define CMD_QUEUE -52164368
#define CMD_PUSH 1069254648
#define CMD_POP 638676238
#define CMD_TTL 320309783


extern int16_t _dispatch_errno;

// server methods.
int32_t dispatch(foo_kv_server *server, int32_t connid, const uint8_t *buff, int32_t len, struct response_t *response);
void error_handler(struct response_t *response);
int32_t do_get(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response);
int32_t do_set(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response);
int32_t do_del(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response);
int32_t do_queue(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response);
int32_t do_push(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response);
int32_t do_pop(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response);
int32_t do_ttl(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response);

// helper methods
PyObject *dumps_as_pyobject(PyObject *x);
const char *dumps(PyObject *x);
PyObject *_dumps_long(PyObject *x);
PyObject *_dumps_float(PyObject *x);
PyObject *_dumps_unicode(PyObject *x);
PyObject *_dumps_list(PyObject *x);
PyObject *_dumps_datetime(PyObject *x);
PyObject *_dumps_hashable_as_pyobject(PyObject *x);
PyObject *_dumps_collectable_as_pyobject(PyObject *x);
PyObject *_dumps_datetime_as_pyobject(PyObject *x);
PyObject *loads_from_pyobject(PyObject *x);
PyObject *loads(const char *x, int32_t len);
PyObject *_loads_long(const char *x, int32_t len);
PyObject *_loads_float(const char *x, int32_t len);
PyObject *_loads_unicode(const char *x, int32_t len);
PyObject *_loads_list(const char *x, int32_t len);
PyObject *_loads_bool(const char *x, int32_t len);
PyObject *_loads_datetime(const char *x, int32_t len);
PyObject *_loads_hashable(const char *x, int32_t len);
PyObject *_loads_hashable_from_pyobject(PyObject *x);
PyObject *_loads_collectable(const char *x, int32_t len);
PyObject *_loads_collectable_from_pyobject(PyObject *x);
PyObject *_loads_foo_datetime(const char *x, int32_t len);
PyObject *_loads_foo_datetime_from_pyobject(PyObject *x);

int32_t _threading_lock_acquire(PyObject *lock);
int32_t _threading_lock_acquire_block(PyObject *lock);
int32_t _threading_lock_release(PyObject *lock);

#endif
