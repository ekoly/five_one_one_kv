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

int16_t _dispatch_errno = 0;

int32_t dispatch(foo_kv_server *server, int32_t connid, const uint8_t *buff, int32_t len, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    log_debug("dispatch(): got request");

    if (!response) {
        log_error("dispatch(): do not have a response object!");
        return -1;
    }
    #endif

    uint16_t nstrs;
    memcpy(&nstrs, buff, sizeof(uint16_t));

    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "dispatch(): nstrs=%d", nstrs);
    log_debug(debug_buffer);
    #endif

    const uint8_t *subcmds[nstrs];
    uint16_t subcmd_to_len[nstrs];
    uint16_t offset = sizeof(uint16_t);
    int32_t err = 0;

    for (int32_t ix = 0; ix < nstrs; ix++) {
        // sanity
        if (offset >= len) {
            log_error("dispatch(): got misformed request.");
            response->status = RES_ERR_CLIENT;
            return 0;
        }
        // establish str len
        uint16_t slen;
        memcpy(&slen, buff + offset, sizeof(uint16_t));
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
        offset += sizeof(uint16_t);
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

    if (PyErr_Occurred()) {
        log_warning("dispatch(): a handler raised a Python exception that was not cleared");
        PyErr_Clear();
    }

    _dispatch_errno = 0;

    return err;

}

void error_handler(struct response_t *response) {
    PyObject *py_err = PyErr_Occurred();
    if (py_err) {
        PyErr_Clear();
    }
    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    sprintf(debug_buffer, "error_handler: got dispatch_errno: %hd", _dispatch_errno);
    log_error(debug_buffer);
    #endif
    switch (_dispatch_errno) {
        case RES_BAD_HASH:
            log_error("error_handler(): Failed to loads(key): not hashable");
            response->status = RES_BAD_HASH;
            break;
        case RES_BAD_TYPE:
            log_error("error_handler(): Failed to loads(key): bad type");
            response->status = RES_BAD_TYPE;
            break;
        case RES_BAD_COLLECTION:
            log_error("error_handler(): Failed to loads(key): embedded collection");
            response->status = RES_BAD_COLLECTION;
            break;
        default:
            #if _FOO_KV_DEBUG == 1
            sprintf(debug_buffer, "error_handler(): Failed to loads(key): unexpected py error type: %hd", _dispatch_errno);
            log_error(debug_buffer);
            #else
            log_error("error_handler(): Failed to loads(key): unexpected py error type");
            #endif
            response->status = RES_UNKNOWN;
    }
}

int32_t do_get(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_get(): got request");
    #endif

    if (nargs != 1) {
        response->status = RES_BAD_ARGS;
        return 0;
    }

    PyObject *loaded_key = _loads_hashable((char *)args[0], arg_to_len[0]);
    if (!loaded_key) {
        error_handler(response);
        return 0;
    }

    // this returns a BORROWED REFERENCE, do not decref
    PyObject *py_val = PyDict_GetItem(server->storage, loaded_key);
    Py_DECREF(loaded_key);

    if (!py_val) {
        #if _FOO_KV_DEBUG == 1
        log_debug("do_get(): Failed to lookup key in storage, perhaps this is expected.");
        #endif
        PyErr_Clear();
        response->status = RES_BAD_KEY;
        return 0;
    }

    PyObject *py_res = dumps_as_pyobject(py_val);
    if (!py_res) {
        error_handler(response);
        return 0;
    }

    response->status = RES_OK;
    response->payload = py_res;

    return 0;

}

int32_t do_put(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_put(): got request");
    #endif

    if (nargs != 2) {
        response->status = RES_BAD_ARGS;
        return 0;
    }

    PyObject *loaded_key = _loads_hashable((char *)args[0], arg_to_len[0]);
    if (!loaded_key) {
        error_handler(response);
        return 0;
    }

    PyObject *loaded_val = loads((char *)args[1], arg_to_len[1]);
    if (!loaded_val) {
        error_handler(response);
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

int32_t do_del(foo_kv_server *server, const uint8_t **args, const uint16_t *arg_to_len, int32_t nargs, struct response_t *response) {

    #if _FOO_KV_DEBUG == 1
    log_debug("do_del(): got request");
    #endif

    if (nargs != 1) {
        response->status = RES_BAD_ARGS;
        return 0;
    }

    int32_t res;

    PyObject *loaded_key = _loads_hashable((char *)args[0], arg_to_len[0]);
    if (!loaded_key) {
        error_handler(response);
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
    /*
    res = _pyobject_safe_delitem(server->storage, loaded_key);
    Py_DECREF(loaded_key);
    */
    res = PyDict_DelItem(server->storage, loaded_key);

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

    /*
    if (res < 0) {
        log_error("do_del(): py operation resulted in error");
        PyErr_Clear();
        response->status = RES_ERR_SERVER;
        return -1;
    }
    */

    if (res < 0) {
        #if _FOO_KV_DEBUG == 1
        log_debug("do_del(): key was not in storage: perhaps this is expected");
        #endif
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }
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

    Py_INCREF(_type_f);
    PyObject *x_type = PyObject_CallOneArg(_type_f, x);
    Py_DECREF(_type_f);
    if (!x_type) {
        return NULL;
    }
    Py_INCREF(_type_to_symbol);
    PyObject *symbol = PyDict_GetItem(_type_to_symbol, x_type);
    Py_DECREF(_type_to_symbol);
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
            _dispatch_errno = RES_BAD_TYPE;
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
        PyErr_Clear();
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
    uint16_t size = PyList_GET_SIZE(x);
    char symbol = LIST_SYMBOL;
    PyObject *intermediate = PyList_New(size);
    if (!intermediate) {
        return NULL;
    }
    #if _FOO_KV_DEBUG == 1
    char debug_buff[256];
    #endif
    // char for type declaration, int16 for the number of items
    int32_t buffer_len = sizeof(char) + sizeof(uint16_t);
    for (Py_ssize_t ix = 0; ix < size; ix++) {
        PyObject *dumped_item = _dumps_collectable_as_pyobject(PyList_GET_ITEM(x, ix));
        if (!dumped_item) {
            Py_DECREF(intermediate);
            return NULL;
        }
        PyList_SET_ITEM(intermediate, ix, dumped_item);
        buffer_len += sizeof(uint16_t) + PyBytes_GET_SIZE(dumped_item);
    }
    char buffer[buffer_len];
    int32_t offset = 0;
    memcpy(buffer + offset, &symbol, sizeof(char));
    offset += sizeof(char);
    memcpy(buffer + offset, &size, sizeof(uint16_t));
    offset += sizeof(uint16_t);
    for (Py_ssize_t ix = 0; ix < size; ix++) {
        #if _FOO_KV_DEBUG == 1
        sprintf(debug_buff, "_dumps_list(): offset: %d", offset);
        log_debug(debug_buff);
        #endif
        PyObject *item = PyList_GET_ITEM(intermediate, ix);
        uint16_t slen = PyBytes_GET_SIZE(item);
        #if _FOO_KV_DEBUG == 1
        sprintf(debug_buff, "_dumps_list(): len(items[%ld])=%hu", ix, slen);
        log_debug(debug_buff);
        sprintf(debug_buff, "_dumps_list(): items[%ld]=%.*s", ix, slen, PyBytes_AS_STRING(item));
        log_debug(debug_buff);
        #endif
        memcpy(buffer + offset, &slen, sizeof(uint16_t));
        offset += sizeof(uint16_t);
        memcpy(buffer + offset, PyBytes_AS_STRING(item), slen);
        offset += slen;
    }

    if (offset != buffer_len) {
        #if _FOO_KV_DEBUG == 1
        if (offset > buffer_len) {
            log_error("_dumps_list(): offset overshot expected buffer_len");
        } else {
            log_error("_dumps_list(): offset undershot expected buffer_len");
        }
        #endif
        _dispatch_errno = RES_ERR_SERVER;
        return NULL;
    }

    Py_DECREF(intermediate);

    return PyBytes_FromStringAndSize(buffer, buffer_len);

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
            _dispatch_errno = RES_BAD_HASH;
            return NULL;
        default:
            _dispatch_errno = RES_BAD_TYPE;
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
            _dispatch_errno = RES_BAD_COLLECTION;
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
            _dispatch_errno = RES_BAD_TYPE;
            return NULL;
    }

    return NULL;

}


PyObject *loads_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    int32_t len = PyObject_Length(x);
    if (!len) {
        _dispatch_errno = RES_BAD_TYPE;
        return NULL;
    }

    return loads(res, len);

}

PyObject *loads(const char *x, int32_t len) {

    switch (x[0]) {
        case INT_SYMBOL:
            return _loads_long(x + 1, len - 1);
        case FLOAT_SYMBOL:
            return _loads_float(x + 1, len - 1);
        case STRING_SYMBOL:
            return _loads_unicode(x + 1, len - 1);
        case BYTES_SYMBOL:
            return PyBytes_FromStringAndSize(x + 1, len - 1);
        case LIST_SYMBOL:
            return _loads_list(x + 1, len - 1);
        case BOOL_SYMBOL:
            return _loads_bool(x + 1, len - 1);
        default:
            _dispatch_errno = RES_BAD_TYPE;
            return NULL;
    }

    return NULL;

}

PyObject *_loads_long(const char *x, int32_t len) {

    PyObject *xs = PyUnicode_FromStringAndSize(x, len);
    if (!xs) {
        return NULL;
    }

    return PyLong_FromUnicodeObject(xs, 0);

}

PyObject *_loads_float(const char *x, int32_t len) {
    PyObject *intermediate = PyUnicode_FromStringAndSize(x, len);
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

PyObject *_loads_unicode(const char *x, int32_t len) {
    return PyUnicode_DecodeUTF8(x, len, "strict");

}

PyObject *_loads_bool(const char *x, int32_t len) {

    if (len != 1) {
        _dispatch_errno = RES_BAD_TYPE;
        return NULL;
    }
    if (x[0] == '0') {
        Py_RETURN_FALSE;
    }
    if (x[0] == '1') {
        Py_RETURN_TRUE;
    }

    _dispatch_errno = RES_BAD_TYPE;
    return NULL;

}

PyObject *_loads_list(const char *x, int32_t len) {

    if ((uint16_t)len < sizeof(uint16_t)) {
        _dispatch_errno = RES_BAD_TYPE;
        return NULL;
    }

    uint16_t nstrs;
    memcpy(&nstrs, x, sizeof(uint16_t));

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    sprintf(debug_buffer, "_loads_list(): nstrs=%hu, len=%d", nstrs, len);
    log_debug(debug_buffer);
    #endif

    const char *items[nstrs];
    uint16_t item_to_len[nstrs];
    uint16_t offset = sizeof(uint16_t);

    for (int32_t ix = 0; ix < nstrs; ix++) {
        #if _FOO_KV_DEBUG == 1
        sprintf(debug_buffer, "_loads_list(): offset=%hu", offset);
        log_debug(debug_buffer);
        #endif
        // sanity
        if (offset >= len) {
            log_error("_loads_list(): got misformed request.");
            _dispatch_errno = RES_ERR_CLIENT;
            return NULL;
        }
        // establish str len
        uint16_t slen;
        memcpy(&slen, x + offset, sizeof(uint16_t));
        if (slen < 0) {
            log_error("_loads_list(): got request item with negative len");
            _dispatch_errno = RES_ERR_CLIENT;
            return NULL;
        }
        item_to_len[ix] = slen;

        #if _FOO_KV_DEBUG == 1
        sprintf(debug_buffer, "_loads_list(): item_to_len[%d]=%d", (int)ix, slen);
        log_debug(debug_buffer);
        #endif

        // establish subcmd
        offset += sizeof(uint16_t);
        items[ix] = x + offset;

        offset += slen;

        #if _FOO_KV_DEBUG == 1
        if (slen < 200) {
            sprintf(debug_buffer, "_loads_list(): items[%d]=%.*s", (int)ix, slen, items[ix]);
            log_debug(debug_buffer);
        } else {
            log_debug("_loads_list(): subcmd too long for log");
        }
        #endif

    }

    if (offset != len) {
        if (offset > len) {
            log_error("dispatch(): got malformed request: offset overshot len");
        } else {
            log_error("dispatch(): got malformed request: offset undershot len");
        }
        _dispatch_errno = RES_ERR_CLIENT;
        return NULL;
    }

    PyObject *result = PyList_New(nstrs);
    for (Py_ssize_t ix = 0; ix < nstrs; ix++) {
        PyObject *loaded_item = _loads_collectable(items[ix], item_to_len[ix]);
        if (!loaded_item) {
            Py_DECREF(result);
            return NULL;
        }
        PyList_SET_ITEM(result, ix, loaded_item);
    }
    return result;

    /*
    PyObject *xs = PyUnicode_FromStringAndSize(x, len);
    PyObject *intermediate = PyObject_CallOneArg(_json_loads_f, xs);
    Py_DECREF(xs);
    if (!intermediate) {
        return NULL;
    }
    if (!PyList_Check(intermediate)) {
        _dispatch_errno = RES_BAD_TYPE;
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
    */

}


PyObject *_loads_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    int32_t len = PyObject_Length(x);
    if (!len) {
        _dispatch_errno = RES_BAD_TYPE;
        return NULL;
    }

    return loads(res, len);

}


PyObject *_loads_hashable(const char *x, int32_t len) {

    switch (x[0]) {
        case INT_SYMBOL:
            return _loads_long(x + 1, len - 1);
        case FLOAT_SYMBOL:
            return _loads_float(x + 1, len - 1);
        case STRING_SYMBOL:
            return _loads_unicode(x + 1, len - 1);
        case BYTES_SYMBOL:
            return PyBytes_FromStringAndSize(x + 1, len - 1);
        case LIST_SYMBOL:
        case BOOL_SYMBOL:
            _dispatch_errno = RES_BAD_HASH;
            return NULL;
        default:
            _dispatch_errno = RES_BAD_TYPE;
            return NULL;
    }

    return NULL;

}


PyObject *_loads_hashable_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    int32_t len = PyObject_Length(x);
    if (!len) {
        _dispatch_errno = RES_BAD_TYPE;
        return NULL;
    }

    return _loads_hashable(res, len);

}


PyObject *_loads_collectable(const char *x, int32_t len) {

    switch (x[0]) {
        case INT_SYMBOL:
            return _loads_long(x + 1, len - 1);
        case FLOAT_SYMBOL:
            return _loads_float(x + 1, len - 1);
        case STRING_SYMBOL:
            return _loads_unicode(x + 1, len - 1);
        case BYTES_SYMBOL:
            return PyBytes_FromStringAndSize(x + 1, len - 1);
        case LIST_SYMBOL:
            _dispatch_errno = RES_BAD_COLLECTION;
            return NULL;
        case BOOL_SYMBOL:
            return _loads_bool(x + 1, len - 1);
        default:
            _dispatch_errno = RES_BAD_TYPE;
            return NULL;
    }

    return NULL;

}


PyObject *_loads_collectable_from_pyobject(PyObject *x) {

    char *res = PyBytes_AsString(x);
    if (!res) {
        return NULL;
    }

    int32_t len = PyObject_Length(x);
    if (!len) {
        _dispatch_errno = RES_BAD_COLLECTION;
        return NULL;
    }

    return _loads_collectable(res, len);

}
