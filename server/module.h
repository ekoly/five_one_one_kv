
#ifndef _FOO_KV_MODULE_H
#define _FOO_KV_MODULE_H

// CHANGE ME
#define _FOO_KV_DEBUG 0
// there is a consistently reproducible segfault when _FOO_KV_POLL_DEBUG is 0
#define _FOO_KV_POLL_DEBUG 1
// _FOO_KV_IO_DEBUG can be left as 0 in most circumstances
#define _FOO_KV_IO_DEBUG 1

#include <Python.h>

#include "server.h"

// python server methods
static void *poll_loop(foo_kv_server *kv_self);
static void *io_loop(foo_kv_server *kv_self);
static PyObject *foo_kv_server_tp_method_poll_loop(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
static PyObject *foo_kv_server_tp_method_io_loop(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

// python util methods
static PyObject *foo_kv_function_dumps(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
static PyObject *foo_kv_function_loads(PyObject *self, PyObject *const *args, Py_ssize_t nargs);
static PyObject *foo_kv_function_hash(PyObject *self, PyObject *const *args, Py_ssize_t nargs);

// allocation method declarations
static PyObject *foo_kv_server_tp_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs);
static void foo_kv_server_tp_clear(foo_kv_server *self);
static void foo_kv_server_tp_dealloc(foo_kv_server *self);
static int foo_kv_server_tp_init(foo_kv_server *self, PyObject *args, PyObject *kwargs);

#endif
