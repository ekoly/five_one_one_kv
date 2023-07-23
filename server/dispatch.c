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

struct Response *dispatch(foo_kv_server *server, int32_t connid, const uint8_t *buff, int32_t len) {

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    log_debug("dispatch(): got request");
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

    for (int32_t ix = 0; ix < nstrs; ix++) {
        // sanity
        if (offset >= len) {
            log_error("dispatch(): got misformed request.");
            return NULL;
        }
        // establish str len
        int32_t slen;
        memcpy(&slen, buff + offset, 4);
        if (slen < 0) {
            log_error("dispatch(): got request subcmd with negative len");
            return NULL;
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
        sprintf(debug_buffer, "dispatch(): subcmds[%d]=%.*s", (int)ix, slen, subcmds[ix]);
        log_debug(debug_buffer);
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
        return NULL;
    }

    int32_t cmd_hash = hash_given_len(subcmds[0], subcmd_to_len[0]);
    int32_t err;
    uint8_t *out;
    struct Response *response = (struct Response *)malloc(sizeof(struct Response));

    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "dispatch(): cmd=%.*s hash=%d", subcmd_to_len[0], subcmds[0], cmd_hash);
    log_debug(debug_buffer);
    #endif

    switch (cmd_hash) {
        case CMD_GET:
            if (nstrs != 2) {
                log_error("dispatch(): got invalid number of args to GET");
                return NULL;
            }
            out = do_get(server, subcmds[1], subcmd_to_len[1]);
            if (!out) {
                if (PyErr_Occurred()) {
                    PyErr_Clear();
                    log_error("dispatch(): got err from do_get()");
                    return NULL;
                }
                response->status = RES_NX;
                response->data = NULL;
                response->datalen = 0;
            } else {
                response->status = RES_OK;
                response->data = out;
                response->datalen = strlen((char *)out);
            }
            break;
        case CMD_PUT:
            if (nstrs != 3) {
                log_error("dispatch(): got invalid number of args to PUT");
                return NULL;
            }
            err = do_put(server, subcmds[1], subcmd_to_len[1], subcmds[2], subcmd_to_len[2]);
            if (err == RES_ERR) {
                log_error("dispatch(): got err from do_put()");
                return NULL;
            } else {
                response->status = RES_OK;
                response->data = NULL;
                response->datalen = 0;
            }
            break;
        case CMD_DEL:
            if (nstrs != 2) {
                log_error("dispatch(): got invalid number of args to DEL");
                return NULL;
            }
            err = do_del(server, subcmds[1], subcmd_to_len[1]);
            if (err == RES_ERR) {
                log_error("dispatch(): got err from do_del()");
                return NULL;
            }
            if (err == RES_NX) {
                response->status = RES_NX;
                response->data = NULL;
                response->datalen = 0;
            } else {
                response->status = RES_OK;
                response->data = NULL;
                response->datalen = 0;
            }
            break;
        default:
            log_error("dispatch(): got unrecognized command");
            return NULL;
    }

    return response;

}

uint8_t *do_get(foo_kv_server *server, const uint8_t *key, int32_t klen) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_get(): got request");
    #endif

    PyObject *py_key = PyBytes_FromStringAndSize((char *)key, klen);
    if (!py_key) {
        log_error("do_get(): Failed to cast key to a PyObject.");
        return NULL;
    }

    PyObject *loaded_key = loads_from_pyobject(py_key);
    Py_DECREF(py_key);
    if (!loaded_key) {
        log_error("do_get(): Failed to loads(key)");
        return NULL;
    }

    PyObject *py_val = PyDict_GetItem(server->storage, loaded_key);
    Py_DECREF(loaded_key);

    if (!py_val) {
        #if _FOO_KV_DEBUG == 1
        log_debug("do_get(): Failed to lookup key in storage, perhaps this is expected.");
        #endif
        return NULL;
    }

    #if _FOO_KV_DEBUG == 1
    uint8_t *res = (uint8_t *)dumps(py_val);
    char debug_buffer[256];
    sprintf(debug_buffer, "do_get(): found result: %s", res);
    log_debug(debug_buffer);
    return res;
    #else
    return (uint8_t *)dumps(py_val);
    #endif

}

int32_t do_put(foo_kv_server *server, const uint8_t *key, int32_t klen, const uint8_t *val, int32_t vlen) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_put(): got request");
    #endif

    PyObject *py_key = PyBytes_FromStringAndSize((char *)key, klen);
    if (!py_key) {
        log_error("do_put(): failed to cast key as py object");
        return RES_ERR;
    }

    PyObject *py_val = PyBytes_FromStringAndSize((char *)val, vlen);
    if (!py_val) {
        log_error("do_put(): failed to cast val as py object");
        return RES_ERR;
    }

    PyObject *loaded_key = loads_from_pyobject(py_key);
    Py_DECREF(py_key);
    if (!loaded_key) {
        log_error("do_put(): failed to loads(key)");
        return RES_ERR;
    }

    PyObject *loaded_val = loads_from_pyobject(py_val);
    Py_DECREF(py_val);
    if (!loaded_val) {
        log_error("do_put(): failed to loads(val)");
        return RES_ERR;
    }

    int32_t has_lock, is_released;
    has_lock = _threading_lock_acquire_block(server->storage_lock);
    if (has_lock < 0) {
        log_error("do_put(): encountered error trying to acquire storage lock");
        return RES_ERR;
    }
    if (!has_lock) {
        log_error("do_put(): failed to acquire_lock");
        return RES_ERR;
    }

    PyDict_SetItem(server->storage, loaded_key, loaded_val);

    Py_DECREF(loaded_key);
    Py_DECREF(loaded_val);

    is_released = _threading_lock_release(server->storage_lock);
    if (is_released < 0) {
        log_error("do_put(): failed to release lock");
        return RES_ERR;
    }

    return RES_OK;

}

int32_t do_del(foo_kv_server *server, const uint8_t *key, int32_t klen) {

    int32_t res;

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    sprintf(debug_buffer, "do_del(): got request with len: %d", klen);
    log_debug(debug_buffer);
    #endif

    PyObject *py_key = PyBytes_FromStringAndSize((char *)key, klen);
    if (!py_key) {
        log_error("do_del(): failed to cast key to py object");
        return RES_ERR;
    }

    PyObject *loaded_key = loads_from_pyobject(py_key);
    Py_DECREF(py_key);
    if (!loaded_key) {
        log_error("do_del(): failed to loads(key)");
        return RES_ERR;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got past py_key");

    if (server->storage == NULL) {
        log_error("do_del(): server storage has become NULL!!!");
        return RES_ERR;
    }
    #endif

    res = PyDict_Contains(server->storage, loaded_key);
    if (res < 0) {
        log_error("do_del(): server storage contains returned error!");
        return RES_ERR;
    }
    if (res == 0) {
        log_debug("do_del(): storage does not contain key, perhaps this is expected");
        return RES_NX;
    }
    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): storage contains key");
    #endif

    int32_t has_lock, is_released;
    has_lock = _threading_lock_acquire_block(server->storage_lock);
    if (has_lock < 0) {
        log_error("do_del(): encountered error trying to acquire storage lock");
        return RES_ERR;
    }
    if (!has_lock) {
        log_error("do_del(): failed to acquire storage lock");
        return RES_ERR;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got past has_lock");
    #endif

    res = PyDict_DelItem(server->storage, loaded_key);
    Py_DECREF(loaded_key);

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got res");
    #endif

    is_released = _threading_lock_release(server->storage_lock);
    if (is_released < 0) {
        log_error("do_del(): failed to release storage lock");
        return RES_ERR;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got past release lock");
    #endif

    if (res < 0) {
        #if _FOO_KV_DEBUG == 1
        log_debug("do_del(): key was not in storage: perhaps this is expected");
        #endif
        PyErr_Clear();
        return RES_NX;
    }


    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): sending successful response");
    #endif

    return RES_OK;

}


// helper methods
PyObject *dumps_as_pyobject(PyObject *x) {

    if (ensure_py_deps()) {
        return NULL;
    }

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
        case '#':
            return _dumps_long(x);
        case '%':
            return _dumps_float(x);
        case '"':
            return _dumps_unicode(x);
        case '\'':
            return PyBytes_FromFormat("%c%s", s, PyBytes_AS_STRING(x));
        case '*':
            return _dumps_list(x);
        case '?':
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
    return PyBytes_FromFormat("%c%ld", '#', l);
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
    PyObject *res = PyBytes_FromFormat("%c%s", '%', PyBytes_AS_STRING(db));
    Py_DECREF(db);
    return res;
}

PyObject *_dumps_unicode(PyObject *x) {
    PyObject *b = PyUnicode_AsUTF8String(x);
    if (!b) {
        return NULL;
    }
    return PyBytes_FromFormat("%c%s", '"', PyBytes_AS_STRING(b));
}

PyObject *_dumps_list(PyObject *x) {
    Py_ssize_t size = PyList_GET_SIZE(x);
    PyObject *intermediate = PyList_New(size);
    for (Py_ssize_t ix = 0; ix < size; ix++) {
        PyObject *dumped_item = dumps_as_pyobject(PyList_GET_ITEM(x, ix));
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
    PyObject *res = PyBytes_FromFormat("%c%s", '*', PyBytes_AS_STRING(dumped_bytes));
    Py_DECREF(dumped_bytes);
    return res;
}

PyObject *loads_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    return loads(res);

}

PyObject *loads(const char *x) {

    switch (x[0]) {
        case '#':
            return PyLong_FromString(x + 1, NULL, 0);
        case '%':
            return _loads_float(x + 1);
        case '"':
            return _loads_unicode(x + 1);
        case '\'':
            return PyBytes_FromString(x + 1);
        case '*':
            return _loads_list(x + 1);
        case '?':
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
    if (ensure_py_deps()) {
        return NULL;
    }
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
        PyObject *loaded_item = loads_from_pyobject(item_bytes);
        Py_DECREF(item_bytes);
        if (!loaded_item) {
            return NULL;
        }
        PyList_SET_ITEM(res, ix, loaded_item);
    }
    Py_DECREF(intermediate);
    return res;
}
