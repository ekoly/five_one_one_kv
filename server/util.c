#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/time.h>

#include "util.h"

// cached python objects
int32_t is_py_deps_init = 0;
PyObject *_builtins = NULL;
PyObject *_asyncio_module = NULL;
PyObject *_queue_class = NULL;
PyObject *_put_nowait_str = NULL;
PyObject *_logging_module = NULL;
PyObject *_logger = NULL;
PyObject *_error_str = NULL;
PyObject *_warning_str = NULL;
PyObject *_info_str = NULL;
PyObject *_debug_str = NULL;
PyObject *_collections_module = NULL;
PyObject *_deq_class = NULL;
PyObject *_pop_str = NULL;
PyObject *_push_str = NULL;
PyObject *_json_module = NULL;
PyObject *_threading_module = NULL;
PyObject *_threading_lock = NULL;
PyObject *_threading_cond = NULL;
PyObject *_threading_sem = NULL;
PyObject *_acquire_str = NULL;
PyObject *_release_str = NULL;
PyObject *_locked_str = NULL;
PyObject *_notify_str = NULL;
PyObject *_wait_str = NULL;
PyObject *_acquire_kwargs_noblock = NULL;
PyObject *_acquire_kwargs_timeout = NULL;
PyObject *_acquire_kwargs_block = NULL;
PyObject *_type_f = NULL;
PyObject *_items = NULL;
PyObject *_type_to_symbol = NULL;
PyObject *_string_class = NULL;
PyObject *_int_symbol = NULL;
PyObject *_float_symbol = NULL;
PyObject *_bytes_symbol = NULL;
PyObject *_string_symbol = NULL;
PyObject *_list_symbol = NULL;
PyObject *_bool_symbol = NULL;
PyObject *_json_kwargs = NULL;
PyObject *_json_loads_f = NULL;
PyObject *_json_dumps_f = NULL;
PyObject *_decode_str = NULL;
PyObject *_utf8_str = NULL;
PyObject *_empty_args = NULL;


// error handling
void log_error(const char *msg) {
    PyObject *py_msg = PyUnicode_FromString(msg);
    PyObject *res = PyObject_CallMethodOneArg(_logger, _error_str, py_msg);
    Py_DECREF(py_msg);
    if (res != NULL) {
        Py_DECREF(res);
    }
}

void log_warning(const char *msg) {
    PyObject *py_msg = PyUnicode_FromString(msg);
    PyObject *res = PyObject_CallMethodOneArg(_logger, _warning_str, py_msg);
    Py_DECREF(py_msg);
    if (res != NULL) {
        Py_DECREF(res);
    }
}

void log_info(const char *msg) {
    PyObject *py_msg = PyUnicode_FromString(msg);
    PyObject *res = PyObject_CallMethodOneArg(_logger, _info_str, py_msg);
    Py_DECREF(py_msg);
    if (res != NULL) {
        Py_DECREF(res);
    }
}

void log_debug(const char *msg) {
    PyObject *py_msg = PyUnicode_FromString(msg);
    PyObject *res = PyObject_CallMethodOneArg(_logger, _debug_str, py_msg);
    Py_DECREF(py_msg);
    if (res != NULL) {
        Py_DECREF(res);
    }
}

void die(const char *msg) {
    int err = errno;
    fprintf(stderr, "[%d] %s\n", err, msg);
    abort();
}

// generic
int32_t randint(int32_t min, int32_t max) {

    struct timeval ts;
    gettimeofday(&ts, NULL);
    srand(ts.tv_usec * ts.tv_sec);

    return min + (max - min) * (double)rand() / (double)RAND_MAX + 0.5;

}

int32_t hash_given_len(const uint8_t *s, size_t n) {

    int32_t x = *s << 7;

    for (size_t ix = 0; ix < n; ix++) {
        x = (1000003 * x) ^ s[ix];
    }
    x ^= n;

    if (x == -1) {
        x = -2;
    }

    return x;

}

int32_t hash(const uint8_t *s) {

    int32_t x = *s << 7;
    const uint8_t *p = s;

    while (*p != '\0') {
        x = (1000003 * x) ^ *p++;
    }
    x ^= (p - s);

    if (x == -1) {
        x = -2;
    }

    return x;

}

// connections utility
int32_t conncounter = 1;

int32_t read_full(int fd, char *buff, size_t n) {
    while (n > 0) {
        ssize_t rv = read(fd, buff, n);
        if (rv <= 0) {
            // error, or unexpected EOF
            return -1;
        }
        n -= (size_t)rv;
        buff += rv;
    }
    return 0;
}

int32_t write_all(int fd, const char *buff, size_t n) {
    while (n > 0) {
        ssize_t rv = write(fd, buff, n);
        if (rv <= 0) {
            // error
            return -1;
        }
        n -= (size_t)rv;
        buff += rv;
    }
    return 0;
}

struct Conn *conn_new(int connfd) {

    struct Conn *conn = (struct Conn *)malloc(sizeof(struct Conn));
    if (conn == NULL) {
        close(connfd);
        return NULL;
    }
    conn->fd = connfd;
    conn->state = STATE_REQ_WAITING;
    conn->rbuff_size = 0;
    conn->rbuff_max = DEFAULT_MSG_SIZE;
    conn->wbuff_size = 0;
    conn->wbuff_sent = 0;
    conn->wbuff_max = DEFAULT_MSG_SIZE;
    conn->connid = conncounter++;

    conn->rbuff = calloc(DEFAULT_MSG_SIZE, sizeof(uint8_t));
    conn->wbuff = calloc(DEFAULT_MSG_SIZE, sizeof(uint8_t));

    conn->lock = PyObject_CallNoArgs(_threading_lock);

    return conn;

}

int32_t conn_resize_rbuff(struct Conn *conn, uint32_t newsize) {

    uint8_t *newbuff = (uint8_t *)realloc(conn->rbuff, (newsize + 1) * sizeof(uint8_t));
    if (!newbuff) {
        return -1;
    }

    conn->rbuff = newbuff;
    conn->rbuff_max = newsize;

    return 0;

}

int32_t conn_resize_wbuff(struct Conn *conn, uint32_t newsize) {

    uint8_t *newbuff = (uint8_t *)realloc(conn->wbuff, newsize * sizeof(uint8_t));
    if (!newbuff) {
        return -1;
    }

    conn->wbuff = newbuff;
    conn->wbuff_max = newsize;

    return 0;

}

int32_t conn_flush(struct Conn *conn, uint32_t flushsize) {

    // remove the request from the buffer
    size_t remain = conn->rbuff_size - flushsize;
    if (remain) {
        size_t newsize = CEIL(remain, 1024);
        if (newsize < 4096) {
            newsize = 4096;
        }
        uint8_t *newbuff = calloc(newsize + 1, sizeof(uint8_t));
        if (!newbuff) {
            return -1;
        }
        memcpy(newbuff, conn->rbuff + flushsize, remain);
        free(conn->rbuff);
        conn->rbuff = newbuff;
        conn->rbuff_size = remain;
        conn->rbuff_max = newsize;
    }

    conn->rbuff_size = remain;

    return 0;

}

int32_t conn_write_response(struct Conn *conn, const struct Response *response) {
    // we should never get here if in state STATE_RES_WAITING
    // therefore wbuff should always be flushed

    // 4 for status + rest for data
    int32_t wlen = 4 + response->datalen;
    // additional 4 for initial len
    size_t wbuff_size = 4 + wlen;

    // resize if necessary
    if (wbuff_size > conn->wbuff_max) {
        size_t newsize = CEIL(wbuff_size, 1024);
        if (newsize > MAX_MSG_SIZE) {
            log_error("conn_write_response(): got response larger than max allowed size");
            return -1;
        }
        int32_t err = conn_resize_wbuff(conn, newsize);
        if (err) {
            return err;
        }
    }

    // write the response len
    memcpy(conn->wbuff, &wlen, 4);
    // write the response status
    memcpy(conn->wbuff + 4, &response->status, 4);
    // write the data
    if (response->datalen > 0) {
        memcpy(conn->wbuff + 8, response->data, response->datalen);
    }
    // update `wbuff_size`
    conn->wbuff_size = wbuff_size;

    return 0;

}

// helper for cached python
int32_t ensure_py_deps() {

    if (is_py_deps_init) {
        return 0;
    }

    _builtins = PyEval_GetBuiltins();
    if (_builtins == NULL) {
        return -1;
    }
    _type_f = PyDict_GetItemString(_builtins, "type");
    if (_type_f == NULL) {
        return -1;
    }
    _asyncio_module = PyImport_ImportModule("asyncio");
    if (_asyncio_module == NULL) {
        return -1;
    }
    _queue_class = PyObject_GetAttrString(_asyncio_module, "Queue");
    if (_queue_class == NULL) {
        return -1;
    }
    _put_nowait_str = PyUnicode_FromString("put_nowait");
    if (_put_nowait_str == NULL) {
        return -1;
    }
    _logging_module = PyImport_ImportModule("logging");
    if (_logging_module == NULL) {
        return -1;
    }
    PyObject *getLogger = PyObject_GetAttrString(_logging_module, "getLogger");
    if (getLogger == NULL) {
        return -1;
    }
    PyObject *logger_name = PyUnicode_FromString("five_one_one_kv.c.server");
    _logger = PyObject_CallOneArg(getLogger, logger_name);
    Py_DECREF(getLogger);
    Py_DECREF(logger_name);
    if (_logger == NULL) {
        return -1;
    }
    _error_str = PyUnicode_FromString("error");
    if (_error_str == NULL) {
        return -1;
    }
    _warning_str = PyUnicode_FromString("warning");
    if (_warning_str == NULL) {
        return -1;
    }
    _info_str = PyUnicode_FromString("info");
    if (_info_str == NULL) {
        return -1;
    }
    _debug_str = PyUnicode_FromString("debug");
    if (_debug_str == NULL) {
        return -1;
    }
    _collections_module = PyImport_ImportModule("collections");
    if (_collections_module == NULL) {
        return -1;
    }
    _deq_class = PyObject_GetAttrString(_collections_module, "deque");
    if (_deq_class == NULL) {
        return -1;
    }
    _pop_str = PyUnicode_FromString("popleft");
    if (_pop_str == NULL) {
        return -1;
    }
    _push_str = PyUnicode_FromString("append");
    if (_push_str == NULL) {
        return -1;
    }
    _json_module = PyImport_ImportModule("json");
    if (_json_module == NULL) {
        return -1;
    }

    _threading_module = PyImport_ImportModule("threading");
    if (_threading_module == NULL) {
        return -1;
    }
    _threading_lock = PyObject_GetAttrString(_threading_module, "Lock");
    if (_threading_lock == NULL) {
        return -1;
    }
    _threading_cond = PyObject_GetAttrString(_threading_module, "Condition");
    if (_threading_cond == NULL) {
        return -1;
    }
    _threading_sem = PyObject_GetAttrString(_threading_module, "Semaphore");
    if (_threading_sem == NULL) {
        return -1;
    }

    _acquire_str = PyUnicode_FromString("acquire");
    if (_acquire_str == NULL) {
        return -1;
    }
    _release_str = PyUnicode_FromString("release");
    if (_release_str == NULL) {
        return -1;
    }
    _locked_str = PyUnicode_FromString("locked");
    if (_locked_str == NULL) {
        return -1;
    }
    _acquire_kwargs_noblock = PyDict_New();
    if (_acquire_kwargs_noblock == NULL) {
        return -1;
    }
    PyDict_SetItemString(_acquire_kwargs_noblock, "blocking", Py_False);
    _acquire_kwargs_timeout = PyDict_New();
    if (_acquire_kwargs_timeout == NULL) {
        return -1;
    }
    PyDict_SetItemString(_acquire_kwargs_timeout, "blocking", Py_True);
    PyObject *py_timeout = PyFloat_FromDouble(1.0);
    PyDict_SetItemString(_acquire_kwargs_timeout, "timeout", py_timeout);
    Py_DECREF(py_timeout);
    _acquire_kwargs_block = PyDict_New();
    if (_acquire_kwargs_block == NULL) {
        return -1;
    }
    PyDict_SetItemString(_acquire_kwargs_block, "blocking", Py_True);
    _notify_str = PyUnicode_FromString("notify");
    if (_notify_str == NULL) {
        return -1;
    }
    _wait_str = PyUnicode_FromString("wait");
    if (_wait_str == NULL) {
        return -1;
    }


    PyObject *separators = PyTuple_New(2);
    if (separators == NULL) {
        return -1;
    }
    PyTuple_SET_ITEM(separators, 0, PyUnicode_FromString(","));
    PyTuple_SET_ITEM(separators, 1, PyUnicode_FromString(":"));
    _json_kwargs = PyDict_New();
    if (_json_kwargs == NULL) {
        return -1;
    }
    PyDict_SetItemString(_json_kwargs, "separators", separators);
    Py_DECREF(separators);

    _type_to_symbol = PyDict_New();
    if (_type_to_symbol == NULL) {
        return -1;
    }
    PyObject *int_class = PyDict_GetItemString(_builtins, "int");
    if (int_class == NULL) {
        return -1;
    }
    PyObject *float_class = PyDict_GetItemString(_builtins, "float");
    if (float_class == NULL) {
        return -1;
    }
    PyObject *bytes_class = PyDict_GetItemString(_builtins, "bytes");
    if (bytes_class == NULL) {
        return -1;
    }
    _string_class = PyDict_GetItemString(_builtins, "str");
    if (_string_class == NULL) {
        return -1;
    }
    PyObject *list_class = PyDict_GetItemString(_builtins, "list");
    if (list_class == NULL) {
        return -1;
    }
    PyObject *bool_class = PyDict_GetItemString(_builtins, "bool");
    if (bool_class == NULL) {
        return -1;
    }

    _int_symbol = PyBytes_FromString("#");
    if (_int_symbol == NULL) {
        return -1;
    }
    if (PyDict_SetItem(_type_to_symbol, int_class, _int_symbol)) {
        return -1;
    }
    Py_DECREF(int_class);
    Py_DECREF(_int_symbol);

    _float_symbol = PyBytes_FromString("%");
    if (_float_symbol == NULL) {
        return -1;
    }
    if (PyDict_SetItem(_type_to_symbol, float_class, _float_symbol)) {
        return -1;
    }
    Py_DECREF(float_class);
    Py_DECREF(_float_symbol);

    _bytes_symbol = PyBytes_FromString("'");
    if (_bytes_symbol == NULL) {
        return -1;
    }
    if (PyDict_SetItem(_type_to_symbol, bytes_class, _bytes_symbol)) {
        return -1;
    }
    Py_DECREF(bytes_class);
    Py_DECREF(_bytes_symbol);

    _string_symbol = PyBytes_FromString("\"");
    if (_string_symbol == NULL) {
        return -1;
    }
    if (PyDict_SetItem(_type_to_symbol, _string_class, _string_symbol)) {
        return -1;
    }
    Py_DECREF(_string_symbol);

    _list_symbol = PyBytes_FromString("*");
    if (_list_symbol == NULL) {
        return -1;
    }
    if (PyDict_SetItem(_type_to_symbol, list_class, _list_symbol)) {
        return -1;
    }
    Py_DECREF(list_class);
    Py_DECREF(_list_symbol);

    _bool_symbol = PyBytes_FromString("?");
    if (_bool_symbol == NULL) {
        return -1;
    }
    if (PyDict_SetItem(_type_to_symbol, bool_class, _bool_symbol)) {
        return -1;
    }
    Py_DECREF(bool_class);
    Py_DECREF(_bool_symbol);

    _json_dumps_f = PyObject_GetAttrString(_json_module, "dumps");
    if (_json_dumps_f == NULL) {
        return -1;
    }

    _json_loads_f = PyObject_GetAttrString(_json_module, "loads");
    if (_json_loads_f == NULL) {
        return -1;
    }

    _decode_str = PyUnicode_FromString("decode");
    if (_decode_str == NULL) {
        return -1;
    }

    _utf8_str = PyUnicode_FromString("utf-8");
    if (_utf8_str == NULL) {
        return -1;
    }

    _empty_args = PyTuple_New(0);
    if (_empty_args == NULL) {
        return -1;
    }

    is_py_deps_init = 1;

    return 0;

}

int32_t _threading_lock_acquire(PyObject *lock) {

    PyObject *callable = PyObject_GetAttr(lock, _acquire_str);
    if (!callable) {
        return -1;
    }

    PyObject *is_acquired = PyObject_Call(callable, _empty_args, _acquire_kwargs_noblock);
    Py_DECREF(callable);
    if (!is_acquired) {
        return -1;
    }

    int32_t res = Py_IsTrue(is_acquired);
    Py_DECREF(is_acquired);

    return res;

}

int32_t _threading_lock_acquire_block(PyObject *lock) {

    PyObject *callable = PyObject_GetAttr(lock, _acquire_str);
    if (!callable) {
        return -1;
    }

    PyObject *is_acquired = PyObject_Call(callable, _empty_args, _acquire_kwargs_block);
    Py_DECREF(callable);
    if (!is_acquired) {
        return -1;
    }

    int32_t res = Py_IsTrue(is_acquired);
    Py_DECREF(is_acquired);

    return res;

}

int32_t _threading_lock_release(PyObject *lock) {

    PyObject *callable = PyObject_GetAttr(lock, _release_str);
    if (!callable) {
        return -1;
    }

    PyObject *res = PyObject_CallNoArgs(callable);
    Py_DECREF(callable);
    if (!res) {
        return -1;
    }

    Py_DECREF(callable);

    return 0;

}

int32_t _threading_lock_locked(PyObject *lock) {

    PyObject *callable = PyObject_GetAttr(lock, _locked_str);
    if (!callable) {
        return -1;
    }

    PyObject *res = PyObject_CallNoArgs(callable);
    Py_DECREF(callable);
    if (!res) {
        return -1;
    }

    Py_DECREF(callable);

    return Py_IsTrue(res);

}
