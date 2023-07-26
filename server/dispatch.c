// this is a toy dispatch to prove that the server works.
// contains code intended to be used by both server and client.

#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <math.h>
#include <Python.h>

#include "server.h"
#include "util.h"
#include "dispatch.h"

int32_t dispatch(foo_kv_server *server, int32_t connid, const uint8_t *buff, int32_t len, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    log_debug("dispatch(): got request");

    if (!response) {
        log_error("dispatch(): do not have a response object!");
        return -1;
    }
    #endif

    int32_t nstrs;
    memcpy(&nstrs, buff, 4);

    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "dispatch(): nstrs=%d", nstrs);
    log_debug(debug_buffer);
    #endif

    const uint8_t *subcmds[nstrs];
    int32_t subcmd_to_len[nstrs];
    int32_t offset = 4;
    int32_t err = 0;

    for (int32_t ix = 0; ix < nstrs; ix++) {
        // sanity
        if (offset >= len) {
            log_error("dispatch(): got misformed request.");
            response->status = RES_ERR_CLIENT;
            return 0;
        }
        // establish str len
        int32_t slen;
        memcpy(&slen, buff + offset, 4);
        if (slen < 0) {
            log_error("dispatch(): got request subcmd with negative len");
            response->status = RES_ERR_CLIENT;
            return 0;
        }
        subcmd_to_len[ix] = slen;

        #if _FOO_KV_DEBUG == 1
        sprintf(debug_buffer, "dispatch(): subcmd_to_len[%d]=%d", (int)ix, slen);
        log_debug(debug_buffer);
        #endif

        // establish subcmd
        offset += 4;
        subcmds[ix] = buff + offset;

        offset += slen;

        #if _FOO_KV_DEBUG == 1
        if (slen < 200) {
            sprintf(debug_buffer, "dispatch(): subcmds[%d]=%.*s", (int)ix, slen, subcmds[ix]);
            log_debug(debug_buffer);
        } else {
            log_debug("dispatch(): subcmd too long for log");
        }
        sprintf(debug_buffer, "dispatch(): offset=%d", offset);
        log_debug(debug_buffer);
        #endif

    }

    if (offset != len) {
        if (offset > len) {
            log_error("dispatch(): got misformed request: offset overshot len");
        } else {
            log_error("dispatch(): got misformed request: offset undershot len");
        }
        response->status = RES_ERR_CLIENT;
        return 0;
    }

    int32_t cmd_hash = hash_given_len(subcmds[0], subcmd_to_len[0]);
    response->status = -1;

    #if _FOO_KV_DEBUG == 1
    if (subcmd_to_len[0] < 200) {
        sprintf(debug_buffer, "dispatch(): cmd=%.*s hash=%d", subcmd_to_len[0], subcmds[0], cmd_hash);
        log_debug(debug_buffer);
    } else {
        log_debug("dispatch(): cmd too long for buffer");
    }
    #endif

    Py_INCREF(server);
    Py_INCREF(server->storage);

    switch (cmd_hash) {
        case CMD_GET:
            err = do_get(server, subcmds + 1, subcmd_to_len + 1, nstrs - 1, response);
            break;
        case CMD_PUT:
            err = do_put(server, subcmds + 1, subcmd_to_len + 1, nstrs - 1, response);
            break;
        case CMD_DEL:
            err = do_del(server, subcmds + 1, subcmd_to_len + 1, nstrs - 1, response);
            break;
        default:
            log_error("dispatch(): got unrecognized command");
            response->status = RES_BAD_CMD;
            break;
    }

    Py_DECREF(server->storage);
    Py_INCREF(server);

    if (response->status == -1) {
        log_warning("dispatch(): response status did not get set!");
        response->status = RES_UNKNOWN;
    }

    return err;

}

int32_t do_get(foo_kv_server *server, const uint8_t **args, const int32_t *arg_to_len, int32_t nargs, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_get(): got request");
    #endif

    if (nargs != 1) {
        response->status = RES_BAD_ARGS;
        return 0;
    }

    PyObject *py_key = PyBytes_FromStringAndSize((char *)args[0], arg_to_len[0]);
    if (!py_key) {
        log_error("do_put(): failed to cast key as py object");
        response->status = RES_BAD_TYPE;
        return 0;
    }

    PyObject *loaded_key = _loads_hashable_from_pyobject(py_key);
    Py_DECREF(py_key);
    if (!loaded_key) {
        PyObject *py_err = PyErr_Occurred();
        if (py_err && PyErr_ExceptionMatches(_not_hashable_error)) {
            log_error("do_get(): Failed to loads(key): not hashable");
            response->status = RES_BAD_HASH;
        } else {
            log_error("do_get(): Failed to loads(key): bad type");
            response->status = RES_BAD_TYPE;
        }
        return 0;
    }

    // this returns a BORROWED REFERENCE, do not decref
    PyObject *py_val = PyDict_GetItem(server->storage, loaded_key);
    Py_DECREF(loaded_key);

    if (!py_val) {
        #if _FOO_KV_DEBUG == 1
        log_debug("do_get(): Failed to lookup key in storage, perhaps this is expected.");
        #endif
        response->status = RES_BAD_KEY;
        return 0;
    }

    PyObject *py_res = dumps_as_pyobject(py_val);

    response->status = RES_OK;
    response->data = (uint8_t *)PyBytes_AsString(py_res);
    response->datalen = PyObject_Length(py_res);
    Py_DECREF(py_res);

    return 0;

}

int32_t do_put(foo_kv_server *server, const uint8_t **args, const int32_t *arg_to_len, int32_t nargs, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_put(): got request");
    #endif

    if (nargs != 2) {
        response->status = RES_BAD_ARGS;
        return 0;
    }

    PyObject *py_key = PyBytes_FromStringAndSize((char *)args[0], arg_to_len[0]);
    if (!py_key) {
        log_error("do_put(): failed to cast key as py object");
        response->status = RES_BAD_TYPE;
        return 0;
    }

    PyObject *loaded_key = _loads_hashable_from_pyobject(py_key);
    Py_DECREF(py_key);
    if (!loaded_key) {
        PyObject *py_err = PyErr_Occurred();
        if (py_err && PyErr_ExceptionMatches(_not_hashable_error)) {
            log_error("do_put(): Failed to loads(key): not hashable");
            response->status = RES_BAD_HASH;
        } else {
            log_error("do_put(): Failed to loads(key): bad type");
            response->status = RES_BAD_TYPE;
        }
        return 0;
    }

    PyObject *py_val = PyBytes_FromStringAndSize((char *)args[1], arg_to_len[1]);
    if (!py_val) {
        log_error("do_put(): failed to cast val as py object");
        response->status = RES_BAD_TYPE;
        return 0;
    }

    PyObject *loaded_val = loads_from_pyobject(py_val);
    Py_DECREF(py_val);
    if (!loaded_val) {
        log_error("do_put(): failed to loads(val)");
        response->status = RES_BAD_TYPE;
        return 0;
    }

    if (threadsafe_sem_wait(server->storage_lock)) {
        log_error("do_put(): encountered error trying to acquire storage lock");
        response->status = RES_ERR_SERVER;
        return 0;
    }

    int32_t res = PyDict_SetItem(server->storage, loaded_key, loaded_val);

    Py_DECREF(loaded_key);
    Py_DECREF(loaded_val);

    if (sem_post(server->storage_lock)) {
        log_error("do_put(): failed to release lock");
        response->status = RES_ERR_SERVER;
        return 0;
    }

    if (res) {
        log_error("do_put(): got error setting item in storage: perhaps this is expected");
        response->status = RES_ERR_SERVER;
        return 0;
    }

    response->status = RES_OK;
    return 0;

}

int32_t do_del(foo_kv_server *server, const uint8_t **args, const int32_t *arg_to_len, int32_t nargs, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got request");
    #endif

    if (nargs != 1) {
        response->status = RES_BAD_ARGS;
        return 0;
    }

    int32_t res;

    PyObject *py_key = PyBytes_FromStringAndSize((char *)args[0], arg_to_len[0]);
    if (!py_key) {
        log_error("do_put(): failed to cast key as py object");
        response->status = RES_BAD_TYPE;
        return 0;
    }

    PyObject *loaded_key = _loads_hashable_from_pyobject(py_key);
    Py_DECREF(py_key);
    if (!loaded_key) {
        PyObject *py_err = PyErr_Occurred();
        if (py_err && PyErr_ExceptionMatches(_not_hashable_error)) {
            log_error("do_get(): Failed to loads(key): not hashable");
            response->status = RES_BAD_HASH;
        } else {
            log_error("do_get(): Failed to loads(key): bad type");
            response->status = RES_BAD_TYPE;
        }
        return 0;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got past py_key");

    if (server->storage == NULL) {
        log_error("do_del(): server storage has become NULL!!!");
        response->status = RES_ERR_SERVER;
        return -1;
    }
    #endif

    if (threadsafe_sem_wait(server->storage_lock)) {
        log_error("do_del(): sem_wait() failed");
        response->status = RES_ERR_SERVER;
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got past sem_wait");
    #endif

    /*
    res = PyDict_Contains(server->storage, loaded_key);
    if (res != 1) {
        if (res == 0) {
            log_debug("do_del(): storage does not contain key, perhaps this is expected");
            response->status = RES_BAD_KEY;
        } else {
            log_error("do_del(): server storage contains returned error!");
            response->status = RES_ERR_SERVER;
        }
        if (sem_post(server->storage_lock)) {
            log_error("do_del(): sem_post() failed");
            response->status = RES_ERR_SERVER;
            return -1;
        }
        return 0;
    }  // storage contains key if we got here

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): storage contains key");
    #endif
    */

    // PyDict_DelItem segfaults randomly
    res = _pyobject_safe_delitem(server->storage, loaded_key);
    Py_DECREF(loaded_key);

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got res");
    #endif

    if (sem_post(server->storage_lock)) {
        log_error("do_del(): sem_post() failed");
        response->status = RES_ERR_SERVER;
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got past release lock");
    #endif

    if (res < 0) {
        log_error("do_del(): py operation resulted in error");
        PyErr_Clear();
        response->status = RES_ERR_SERVER;
        return -1;
    }

    if (res == 0) {
        #if _FOO_KV_DEBUG == 1
        log_debug("do_del(): key was not in storage: perhaps this is expected");
        #endif
        response->status = RES_BAD_KEY;
        return 0;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): sending successful response");
    #endif

    response->status = RES_OK;
    return 0;

}


// helper methods
PyObject *dumps_as_pyobject(PyObject *x) {

    PyObject *x_type = PyObject_CallOneArg(_type_f, x);
    if (!x_type) {
        return NULL;
    }
    PyObject *symbol = PyDict_GetItem(_type_to_symbol, x_type);
    Py_DECREF(x_type);
    if (!symbol) {
        return NULL;
    }

    char s = *PyBytes_AS_STRING(symbol);

    switch (s) {
        case INT_SYMBOL:
            return _dumps_long(x);
        case FLOAT_SYMBOL:
            return _dumps_float(x);
        case STRING_SYMBOL:
            return _dumps_unicode(x);
        case BYTES_SYMBOL:
            return PyBytes_FromFormat("%c%s", s, PyBytes_AS_STRING(x));
        case LIST_SYMBOL:
            return _dumps_list(x);
        case BOOL_SYMBOL:
            if (Py_IsTrue(x)) {
                return PyBytes_FromFormat("%c%c", s, '1');
            }
            if (Py_IsFalse(x)) {
                return PyBytes_FromFormat("%c%c", s, '0');
            }
            return NULL;
        default:
            PyErr_SetString(PyExc_TypeError, "unsupported type passed to dumps");
            return NULL;
    }

    return NULL;

}

const char *dumps(PyObject *x) {

    PyObject *res = dumps_as_pyobject(x);
    if (!res) {
        return NULL;
    }
    return PyBytes_AS_STRING(res);

}

PyObject *_dumps_long(PyObject *x) {
    long l = PyLong_AsLong(x);
    if (PyErr_Occurred()) {
        return NULL;
    }
    return PyBytes_FromFormat("%c%ld", INT_SYMBOL, l);
}

PyObject *_dumps_float(PyObject *x) {
    // d for double
    PyObject *d = PyObject_CallOneArg(_string_class, x);
    if (!d) {
        return NULL;
    }
    // db for double as bytes
    PyObject *db = PyUnicode_AsASCIIString(d);
    Py_DECREF(d);
    if (!db) {
        return NULL;
    }
    PyObject *res = PyBytes_FromFormat("%c%s", FLOAT_SYMBOL, PyBytes_AS_STRING(db));
    Py_DECREF(db);
    return res;
}

PyObject *_dumps_unicode(PyObject *x) {
    PyObject *b = PyUnicode_AsUTF8String(x);
    if (!b) {
        return NULL;
    }
    return PyBytes_FromFormat("%c%s", STRING_SYMBOL, PyBytes_AS_STRING(b));
}

PyObject *_dumps_list(PyObject *x) {
    Py_ssize_t size = PyList_GET_SIZE(x);
    PyObject *intermediate = PyList_New(size);
    for (Py_ssize_t ix = 0; ix < size; ix++) {
        PyObject *dumped_item = _dumps_collectable_as_pyobject(PyList_GET_ITEM(x, ix));
        if (!dumped_item) {
            return NULL;
        }
        PyObject *item_str = PyObject_CallMethodOneArg(dumped_item, _decode_str, _utf8_str);
        Py_DECREF(dumped_item);
        if (!item_str) {
            return NULL;
        }
        PyList_SET_ITEM(intermediate, ix, item_str);
    }
    PyObject *dump_args = PyTuple_New(1);
    // this steals a reference to intermediate
    PyTuple_SET_ITEM(dump_args, 0, intermediate);
    PyObject *dumped = PyObject_Call(_json_dumps_f, dump_args, _json_kwargs);
    Py_DECREF(dump_args);
    if (!dumped) {
        return NULL;
    }
    PyObject *dumped_bytes = PyUnicode_AsASCIIString(dumped);
    Py_DECREF(dumped);
    PyObject *res = PyBytes_FromFormat("%c%s", LIST_SYMBOL, PyBytes_AS_STRING(dumped_bytes));
    Py_DECREF(dumped_bytes);
    return res;
}


PyObject *_dumps_hashable_as_pyobject(PyObject *x) {

    PyObject *x_type = PyObject_CallOneArg(_type_f, x);
    if (!x_type) {
        return NULL;
    }
    PyObject *symbol = PyDict_GetItem(_type_to_symbol, x_type);
    Py_DECREF(x_type);
    if (!symbol) {
        return NULL;
    }

    char s = *PyBytes_AS_STRING(symbol);

    switch (s) {
        case INT_SYMBOL:
            return _dumps_long(x);
        case FLOAT_SYMBOL:
            return _dumps_float(x);
        case STRING_SYMBOL:
            return _dumps_unicode(x);
        case BYTES_SYMBOL:
            return PyBytes_FromFormat("%c%s", BYTES_SYMBOL, PyBytes_AS_STRING(x));
        case LIST_SYMBOL:
        case BOOL_SYMBOL:
            PyErr_SetString(_not_hashable_error, "Cannot use the given type as a key");
            return NULL;
        default:
            PyErr_SetString(PyExc_TypeError, "unsupported type passed to dumps");
            return NULL;
    }

    return NULL;

}


PyObject *_dumps_collectable_as_pyobject(PyObject *x) {

    PyObject *x_type = PyObject_CallOneArg(_type_f, x);
    if (!x_type) {
        return NULL;
    }
    PyObject *symbol = PyDict_GetItem(_type_to_symbol, x_type);
    Py_DECREF(x_type);
    if (!symbol) {
        return NULL;
    }

    char s = *PyBytes_AS_STRING(symbol);

    switch (s) {
        case INT_SYMBOL:
            return _dumps_long(x);
        case FLOAT_SYMBOL:
            return _dumps_float(x);
        case STRING_SYMBOL:
            return _dumps_unicode(x);
        case BYTES_SYMBOL:
            return PyBytes_FromFormat("%c%s", s, PyBytes_AS_STRING(x));
        case LIST_SYMBOL:
            PyErr_SetString(_embedded_collection_error, "Cannot embed a collection type inside another collection");
            return NULL;
        case BOOL_SYMBOL:
            if (Py_IsTrue(x)) {
                return PyBytes_FromFormat("%c%c", BOOL_SYMBOL, '1');
            }
            if (Py_IsFalse(x)) {
                return PyBytes_FromFormat("%c%c", BOOL_SYMBOL, '0');
            }
            return NULL;
        default:
            PyErr_SetString(PyExc_TypeError, "unsupported type passed to dumps");
            return NULL;
    }

    return NULL;

}


PyObject *loads_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    if (!PyObject_Length(x)) {
        PyErr_SetString(PyExc_TypeError, "cannot load zero length bytes");
        return NULL;
    }

    return loads(res);

}

PyObject *loads(const char *x) {

    switch (x[0]) {
        case INT_SYMBOL:
            return PyLong_FromString(x + 1, NULL, 0);
        case FLOAT_SYMBOL:
            return _loads_float(x + 1);
        case STRING_SYMBOL:
            return _loads_unicode(x + 1);
        case BYTES_SYMBOL:
            return PyBytes_FromString(x + 1);
        case LIST_SYMBOL:
            return _loads_list(x + 1);
        case BOOL_SYMBOL:
            if (x[1] == '0') {
                Py_RETURN_FALSE;
            }
            if (x[1] == '1') {
                Py_RETURN_TRUE;
            }
            return NULL;
        default:
            PyErr_SetString(PyExc_ValueError, "Tried to load object with unrecognized type.");
            return NULL;
    }

    return NULL;

}

PyObject *_loads_float(const char *x) {
    PyObject *intermediate = PyUnicode_FromString(x);
    if (!intermediate) {
        return NULL;
    }
    PyObject *res = PyFloat_FromString(intermediate);
    Py_DECREF(intermediate);
    if (!res) {
        return NULL;
    }
    return res;
}

PyObject *_loads_unicode(const char *x) {
    Py_ssize_t size = strlen(x);
    return PyUnicode_DecodeUTF8(x, size, "strict");
}

PyObject *_loads_list(const char *x) {
    PyObject *xs = PyUnicode_FromString(x);
    PyObject *intermediate = PyObject_CallOneArg(_json_loads_f, xs);
    Py_DECREF(xs);
    if (!intermediate) {
        return NULL;
    }
    Py_ssize_t size = PyList_GET_SIZE(intermediate);
    PyObject *res = PyList_New(size);
    for (Py_ssize_t ix = 0; ix < size; ix++) {
        PyObject *item_bytes = PyUnicode_AsUTF8String(PyList_GET_ITEM(intermediate, ix));
        if (!item_bytes) {
            return NULL;
        }
        PyObject *loaded_item = _loads_collectable_from_pyobject(item_bytes);
        Py_DECREF(item_bytes);
        if (!loaded_item) {
            return NULL;
        }
        PyList_SET_ITEM(res, ix, loaded_item);
    }
    Py_DECREF(intermediate);
    return res;
}


PyObject *_loads_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    if (!PyObject_Length(x)) {
        PyErr_SetString(PyExc_TypeError, "cannot load zero length bytes");
        return NULL;
    }

    return loads(res);

}


PyObject *_loads_hashable(const char *x) {

    switch (x[0]) {
        case INT_SYMBOL:
            return PyLong_FromString(x + 1, NULL, 0);
        case FLOAT_SYMBOL:
            return _loads_float(x + 1);
        case STRING_SYMBOL:
            return _loads_unicode(x + 1);
        case BYTES_SYMBOL:
            return PyBytes_FromString(x + 1);
        case LIST_SYMBOL:
        case BOOL_SYMBOL:
            PyErr_SetString(_not_hashable_error, "Cannot use the given type as a key");
            return NULL;
        default:
            PyErr_SetString(PyExc_ValueError, "Tried to load object with unrecognized type.");
            return NULL;
    }

    return NULL;

}


PyObject *_loads_hashable_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    if (!PyObject_Length(x)) {
        PyErr_SetString(PyExc_TypeError, "cannot load zero length bytes");
        return NULL;
    }

    return _loads_hashable(res);

}


PyObject *_loads_collectable(const char *x) {

    switch (x[0]) {
        case INT_SYMBOL:
            return PyLong_FromString(x + 1, NULL, 0);
        case FLOAT_SYMBOL:
            return _loads_float(x + 1);
        case STRING_SYMBOL:
            return _loads_unicode(x + 1);
        case BYTES_SYMBOL:
            return PyBytes_FromString(x + 1);
        case LIST_SYMBOL:
            PyErr_SetString(_embedded_collection_error, "Cannot embed a collection type inside another collection");
            return NULL;
        case BOOL_SYMBOL:
            if (x[1] == '0') {
                Py_RETURN_FALSE;
            }
            if (x[1] == '1') {
                Py_RETURN_TRUE;
            }
            return NULL;
        default:
            PyErr_SetString(PyExc_ValueError, "Tried to load object with unrecognized type.");
            return NULL;
    }

    return NULL;

}


PyObject *_loads_collectable_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    if (!PyObject_Length(x)) {
        PyErr_SetString(PyExc_TypeError, "cannot load zero length bytes");
        return NULL;
    }

    return _loads_collectable(res);

}
