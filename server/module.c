// python method definitions
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

#include <Python.h>
#include "structmember.h"

#include "module.h"
#include "util.h"
#include "server.h"
#include "dispatch.h"


// init/dealloc methods for foo_kv_server
static PyObject *foo_kv_server_tp_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs) {

    foo_kv_server *self = (foo_kv_server *)subtype->tp_alloc(subtype, 0);

    return (PyObject *)self;

}

static void foo_kv_server_tp_clear(foo_kv_server *self) {

    Py_DECREF(self->storage);
    Py_DECREF(self->storage_lock);
    Py_DECREF(self->user_locks);
    Py_DECREF(self->user_locks_lock);

    connarray_dealloc(self->fd_to_conn);

}

static void foo_kv_server_tp_dealloc(foo_kv_server *self) {

    foo_kv_server_tp_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);

}

static int foo_kv_server_tp_init(foo_kv_server *self, PyObject *args, PyObject *kwargs) {

    if (ensure_py_deps()) {
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    char debug_buff[256];
    log_debug("got past ensure_py_deps()");
    #endif

    int port, num_threads;

    static char *kwlist[] = {"port", "num_threads", NULL};

    if (!PyArg_ParseTupleAndKeywords(args, kwargs, "ii", kwlist, &port, &num_threads)) {
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buff, "got past arg parsing: got port: %d", port);
    log_debug(debug_buff);
    #endif

    self->storage = PyDict_New();
    if (!self->storage) {
        return -1;
    }
    self->storage_lock = PyObject_CallNoArgs(_threading_lock);
    if (!self->storage_lock) {
        return -1;
    }
    self->user_locks = PyDict_New();
    if (!self->user_locks) {
        return -1;
    }
    self->user_locks_lock = PyObject_CallNoArgs(_threading_lock);
    if (!self->user_locks) {
        return -1;
    }
    self->waiting_conns = PyObject_CallNoArgs(_deq_class);
    if (!self->waiting_conns) {
        return -1;
    }
    self->waiting_conns_lock = PyObject_CallNoArgs(_threading_lock);
    if (!self->waiting_conns_lock) {
        return -1;
    }
    self->io_conns_cond = PyObject_CallNoArgs(_threading_cond);
    if (!self->io_conns_cond) {
        return -1;
    }
    PyObject *num_io_workers = PyLong_FromLong(num_threads - 1);
    self->io_conns_sem = PyObject_CallOneArg(_threading_sem, num_io_workers);
    if (!self->io_conns_sem) {
        return -1;
    }
    Py_DECREF(num_io_workers);

    #if _FOO_KV_DEBUG == 1
    log_debug("got past py member init");
    #endif

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        PyErr_SetString(PyExc_RuntimeError, "socket()");
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("got past socket creation");
    #endif

    self->fd = fd;

    // following is constant for most server applications
    int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    #if _FOO_KV_DEBUG == 1
    log_debug("got past setsockopt()");
    #endif
    
    // bind
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    // port is from user
    addr.sin_port = ntohs(port);
    // 0 -> 0.0.0.0
    addr.sin_addr.s_addr = ntohl(0); 

    #if _FOO_KV_DEBUG == 1
    log_debug("got past setting up addr");
    #endif

    int rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        log_error("failed to bind()");
        PyErr_SetString(PyExc_ValueError, "bind() failed to connect");
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("got past bind()");
    #endif

    // listen
    rv = listen(fd, SOMAXCONN);
    if (rv) {
        log_error("failed to listen()");
        PyErr_SetString(PyExc_ValueError, "listen() failed to start");
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("got past listen()");
    #endif

    self->fd_to_conn = calloc(1, sizeof(struct ConnArray));
    if (connarray_init(self->fd_to_conn, 8)) {
        log_error("calloc() failed to allocate memory");
        return -1;
    }
    
    fd_set_nb(fd);

    return 0;

}


// public client methods
static PyObject *foo_kv_function_dumps(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "wrong number of arguments to `dumps`, expects 1.");
        return NULL;
    }

    return dumps_as_pyobject(args[0]);

}

static PyObject *foo_kv_function_loads(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "wrong number of arguments to `loads`, expects 1.");
        return NULL;
    }

    return loads_from_pyobject(args[0]);

}

static PyObject *foo_kv_function_hash(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "wrong number of arguments to `hash`, expects 1.");
        return NULL;
    }

    const char *buff = PyBytes_AsString(args[0]);
    if (buff == NULL) {
        return NULL;
    }

    int res = hash((uint8_t *)buff);

    return PyLong_FromLong(res);

}

// server public methods
static PyObject *foo_kv_server_tp_method_poll_loop(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 0) {
        PyErr_SetString(PyExc_TypeError, "poll_loop expects no arguments");
        return NULL;
    }

    foo_kv_server *kv_self = (foo_kv_server *)self;
    struct ConnArray *fd_to_conn = kv_self->fd_to_conn;

    nfds_t poll_args_size = 0;
    struct pollfd *poll_args = (struct pollfd *)calloc(1, sizeof(struct pollfd));

    int32_t has_lock, is_released;
    // keep track of number of active connections
    int32_t num_active = 0;
    int32_t poll_timeout = 0;
    PyObject *callable;
    PyObject *py_has_lock;

    // the event loop
    while (1) {

        #if _FOO_KV_DEBUG == 1
        log_debug("poll_loop(): beginning of poll_loop()");
        #endif
        num_active = 0;

        free(poll_args); 
        poll_args = calloc(fd_to_conn->size + 1, sizeof(struct pollfd));
        poll_args_size = 0;

        // for convenience, the listening fd is put in the first position
        struct pollfd pfd = {kv_self->fd, POLLIN, 0};
        poll_args[poll_args_size] = pfd;
        poll_args_size++;

        // connection fds
        for (int32_t ix = 0; ix < fd_to_conn->maxsize; ix++) {
            struct Conn *conn = fd_to_conn->arr[ix];
            if (!conn) {
                continue;
            }
            int events = 0;
            switch (conn->state) {
                case STATE_REQ:
                case STATE_RES:
                case STATE_DISPATCH:
                    num_active++;
                    continue;
                case STATE_END:
                    has_lock = _threading_lock_acquire(conn->lock);
                    if (has_lock < 0) {
                        log_error("poll_loop(): error in acquire lock for ended connection");
                        return NULL;
                    }
                    if (has_lock == 0) {
                        log_warning("poll_loop(): conn lock is locked for ended connection");
                        continue;
                    }
                    connarray_remove(fd_to_conn, conn);
                    continue;
                case STATE_REQ_WAITING:
                    events = POLLIN;
                    break;
                case STATE_RES_WAITING:
                    events = POLLOUT;
                    break;
                default:
                    log_error("poll_loop(): got invalid state");
                    return NULL;
            }
            struct pollfd pfd = {};
            pfd.fd = conn->fd;
            pfd.events = events;
            pfd.events = events | POLLERR;
            poll_args[poll_args_size] = pfd;
            poll_args_size++;

        }

        #if _FOO_KV_DEBUG == 1
        log_debug("poll_loop(): about to call poll()");
        #endif

        // poll for active fds
        // Acquire Sem: this is intended to introduce sleep time if all other
        // threads are busy
        callable = PyObject_GetAttr(kv_self->io_conns_sem, _acquire_str);
        if (callable == NULL) {
            log_error("poll_loop(): getting callable for Semaphore::acquire failed.");
            return NULL;
        }
        py_has_lock = PyObject_Call(callable, _empty_args, _acquire_kwargs_timeout);
        Py_DECREF(callable);
        if (py_has_lock == NULL) {
            log_error("poll_loop(): calling Semaphore::acquire resulted in error.");
            return NULL;
        }
        if (Py_IsTrue(py_has_lock)) {
            Py_DECREF(py_has_lock);
            py_has_lock = PyObject_CallMethodNoArgs(kv_self->io_conns_sem, _release_str);
            if (py_has_lock == NULL) {
                log_error("poll_loop(): calling Semaphore::release resulted in error.");
            }
        }
        Py_DECREF(py_has_lock);
        // if timeout is given, it can block other threads, hence 0 if there are active
        poll_timeout = (num_active > 0) ? 0 : 1000;
        int rv = poll(poll_args, poll_args_size, poll_timeout);
        if (rv < 0) {
            PyErr_SetString(PyExc_RuntimeError, "poll()");
            return NULL;
        }

        #if _FOO_KV_DEBUG == 1
        log_debug("poll_loop(): called poll()");
        #endif

        // process active connections
        // TODO comments
        for (nfds_t ix = 1; ix < poll_args_size; ix++) {
            if (poll_args[ix].revents) {
                struct Conn *conn = fd_to_conn->arr[poll_args[ix].fd];
                if (conn == NULL) {
                    continue;
                }
                has_lock = _threading_lock_acquire(conn->lock);
                if (has_lock < 0) {
                    log_error("poll_loop(): failed to acquire lock for idle connection");
                    return NULL;
                }
                if (has_lock == 0) {
                    log_error("poll_loop(): conn lock is locked for idle connection");
                    continue;
                }
                int32_t is_waiting = 0;
                if (conn->state == STATE_REQ_WAITING) {
                    conn->state = STATE_REQ;
                    is_waiting = 1;
                } else if (conn->state == STATE_RES_WAITING) {
                    conn->state = STATE_RES;
                    is_waiting = 1;
                }
                is_released = _threading_lock_release(conn->lock);
                if (is_released < 0) {
                    log_error("poll_loop(): failed to release lock for connection");
                    return NULL;
                }
                if (is_waiting) {
                    #if _FOO_KV_DEBUG == 1
                    log_debug("poll_loop(): about to acquire waiting_conns_lock");
                    #endif
                    has_lock = _threading_lock_acquire_block(kv_self->waiting_conns_lock);
                    if (has_lock < 0) {
                        log_error("poll_loop(): failed to acquire lock for waiting conns deque");
                        return NULL;
                    }
                    #if _FOO_KV_DEBUG == 1
                    log_debug("poll_loop(): successfully acquired waiting_conns_lock");
                    #endif
                    PyObject *conn_fd = PyLong_FromLong(conn->fd);
                    if (!conn_fd) {
                        log_error("poll_loop(): failed to cast conn fd to a PyLong");
                        return NULL;
                    }
                    PyObject *res = PyObject_CallMethodOneArg(kv_self->waiting_conns, _push_str, conn_fd);
                    Py_DECREF(conn_fd);
                    if (!res) {
                        log_error("poll_loop(): failed to enqueue connection");
                        return NULL;
                    }
                    Py_DECREF(res);

                    is_released = _threading_lock_release(kv_self->waiting_conns_lock);
                    if (is_released < 0) {
                        log_error("poll_loop(): failed to release lock for waiting conns");
                        return NULL;
                    }

                    // notify other threads that a connection is ready
                    py_has_lock = PyObject_CallMethodNoArgs(kv_self->io_conns_cond, _acquire_str);
                    if (py_has_lock == NULL) {
                        log_error("io_loop(): failed to acquire io_conns_cond");
                        return NULL;
                    }
                    Py_DECREF(py_has_lock);
                    py_has_lock = PyObject_CallMethodNoArgs(kv_self->io_conns_cond, _notify_str);
                    if (py_has_lock == NULL) {
                        log_error("io_loop(): failed to wait for io_conns_cond");
                        return NULL;
                    }
                    Py_DECREF(py_has_lock);
                    py_has_lock = PyObject_CallMethodNoArgs(kv_self->io_conns_cond, _release_str);
                    if (py_has_lock == NULL) {
                        log_error("io_loop(): failed to release io_conns_cond");
                        return NULL;
                    }
                } // end of is waiting block
            } // end of is events
        } // end of loop

        #if _FOO_KV_DEBUG == 1
        log_debug("poll_loop(): about to accept new connections");
        #endif

        // accept new connections
        if (poll_args[0].revents) {
            accept_new_conn(fd_to_conn, kv_self->fd);
        }

        #if _FOO_KV_DEBUG == 1
        log_debug("poll_loop(): end of loop");
        #endif

    }

    Py_RETURN_NONE;

}

static PyObject *foo_kv_server_tp_method_io_loop(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 0) {
        PyErr_SetString(PyExc_TypeError, "io_loop() expects no connections.");
        return NULL;
    }

    foo_kv_server *kv_self = (foo_kv_server *)self;
    struct ConnArray *fd_to_conn = kv_self->fd_to_conn;
    int32_t has_lock, is_released;
    PyObject *py_has_lock;

    while (1) {

        #if _FOO_KV_DEBUG == 1
        log_debug("io_loop(): beginning of loop");
        #endif

        // io_conns_cond will be released when there are items on the q
        py_has_lock = PyObject_CallMethodNoArgs(kv_self->io_conns_cond, _acquire_str);
        if (py_has_lock == NULL) {
            log_error("io_loop(): failed to acquire io_conns_cond");
            return NULL;
        }
        Py_DECREF(py_has_lock);
        py_has_lock = PyObject_CallMethodNoArgs(kv_self->io_conns_cond, _wait_str);
        if (py_has_lock == NULL) {
            log_error("io_loop(): failed to wait for io_conns_cond");
            return NULL;
        }
        Py_DECREF(py_has_lock);
        py_has_lock = PyObject_CallMethodNoArgs(kv_self->io_conns_cond, _release_str);
        if (py_has_lock == NULL) {
            log_error("io_loop(): failed to release io_conns_cond");
            return NULL;
        }

        // check if deq still has len
        if (PyObject_Length(kv_self->waiting_conns) == 0) {
            // whoops, another thread got here first, reiterate
            continue;
        }
        has_lock = _threading_lock_acquire_block(kv_self->waiting_conns_lock);
        if (has_lock < 0) {
            log_error("io_loop(): failed to acquire waiting conns lock");
            return NULL;
        }
        // check if deq still has len
        if (PyObject_Length(kv_self->waiting_conns) == 0) {
            // whoops, another thread got here first, reiterate
            is_released = _threading_lock_release(kv_self->waiting_conns_lock);
            if (is_released < 0) {
                log_error("io_loop(): failed to release waiting conns lock after encountering empty deque");
                return NULL;
            }
            continue;
        }
        PyObject *py_conn_fd = PyObject_CallMethodNoArgs(kv_self->waiting_conns, _pop_str);
        is_released = _threading_lock_release(kv_self->waiting_conns_lock);
        if (is_released < 0) {
            log_error("io_loop(): failed to release waiting conns lock after getting from deque");
            return NULL;
        }
        if (!py_conn_fd) {
            log_error("io_loop(): failed get from deque");
            return NULL;
        }
        int32_t conn_fd = PyLong_AsLong(py_conn_fd);
        Py_DECREF(py_conn_fd);
        struct Conn *conn = fd_to_conn->arr[conn_fd];
        if (connection_io(kv_self, conn) < 0) {
            if (PyErr_Occurred()) {
                log_error("io_loop(): connection_io() reported py error");
                return NULL;
            }
            log_error("connection io returned error");
            conn->state = STATE_END;
        }
    }

}

// server public methods
static PyMethodDef foo_kv_server_tp_methods[] = {
    {"poll_loop", _PyCFunction_CAST(foo_kv_server_tp_method_poll_loop), METH_FASTCALL, "Start the loop that listens for connections."},
    {"io_loop", _PyCFunction_CAST(foo_kv_server_tp_method_io_loop), METH_FASTCALL, "Perform IO operations on a fd."},
    {NULL, NULL, 0, NULL}
};

// define our members
static PyMemberDef foo_kv_server_tp_members[] = {
    {"waiting_conns", T_OBJECT_EX, offsetof(foo_kv_server, waiting_conns), READONLY, ""},
    {"waiting_conns_lock", T_OBJECT_EX, offsetof(foo_kv_server, waiting_conns_lock), READONLY, ""},
    {NULL, 0, 0, 0, NULL}
};

// server py class
static PyTypeObject FooKVServerType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "server",                                   /*tp_name*/
    sizeof(foo_kv_server),                      /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)foo_kv_server_tp_dealloc,       /*tp_dealloc*/
    0,                                          /*tp_print*/
    0,                                          /*tp_getattr*/
    0,                                          /*tp_setattr*/
    0,                                          /*tp_compare*/
    0,                                          /*tp_repr*/
    0,                                          /*tp_as_number*/
    0,                                          /*tp_as_sequence*/
    0,                                          /*tp_as_mapping*/
    0,                                          /*tp_hash */
    0,                                          /*tp_call*/
    0,                                          /*tp_str*/
    PyObject_GenericGetAttr,                    /*tp_getattro*/
    PyObject_GenericSetAttr,                    /*tp_setattro*/
    0,                                          /*tp_as_buffer*/
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,   /*tp_flags*/
    0,                                          /*tp_doc*/
    0,                                          /*tp_traverse*/
    (inquiry)foo_kv_server_tp_clear,            /*tp_clear*/
    0,                                          /*tp_richcompare*/
    0,                                          /*tp_weaklistoffset*/
    0,                                          /*tp_iter*/
    0,                                          /*tp_iternext*/
    foo_kv_server_tp_methods,                   /*tp_methods*/
    foo_kv_server_tp_members,                   /*tp_members*/
    0,                                          /*tp_getsets*/
    0,                                          /*tp_base*/
    0,                                          /*tp_dict*/
    0,                                          /*tp_descr_get*/
    0,                                          /*tp_descr_set*/
    0,                                          /*tp_dictoffset*/
    (initproc)foo_kv_server_tp_init,            /*tp_init*/
    0,                                          /*tp_alloc*/
    foo_kv_server_tp_new,                       /*tp_new*/
};

// define our module methods
static PyMethodDef foo_kv_method_def[] = {
    {"dumps", _PyCFunction_CAST(foo_kv_function_dumps), METH_FASTCALL, "Serialize user data."},
    {"loads", _PyCFunction_CAST(foo_kv_function_loads), METH_FASTCALL, "Deserialize user data."},
    {"foo_hash", _PyCFunction_CAST(foo_kv_function_hash), METH_FASTCALL, "Get the hash of the user data."},
    {NULL, NULL, 0, NULL}
};

// define our module
static struct PyModuleDef foo_kv_module_def = {
    PyModuleDef_HEAD_INIT,
    "c",
    "KV store module implemented in C.",
    -1,
    foo_kv_method_def,
};

// register our module and add the public methods to it
PyMODINIT_FUNC PyInit_c(void) {

    PyObject *foo_kv_module = PyModule_Create(&foo_kv_module_def);

    PyModule_AddType(foo_kv_module, &FooKVServerType);

    PyModule_AddIntConstant(foo_kv_module, "RES_OK", RES_OK);
    PyModule_AddIntConstant(foo_kv_module, "RES_ERR", RES_ERR);
    PyModule_AddIntConstant(foo_kv_module, "RES_NX", RES_NX);
    PyModule_AddIntConstant(foo_kv_module, "RES_UNKNOWN", RES_UNKNOWN);

    return foo_kv_module;

}
