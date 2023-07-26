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
#include <semaphore.h>
#include <pthread.h>

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

    #if _FOO_KV_DEBUG == 1
    log_warning("five_one_one_kv.server is being cleared!!!");
    #endif

    Py_DECREF(self->storage);
    Py_DECREF(self->user_locks);
    Py_DECREF(self->user_locks_lock);

    PyMem_RawFree(self->waiting_conns_ready_cond);

    sem_destroy(self->storage_lock);
    sem_destroy(self->waiting_conns_lock);
    PyMem_RawFree(self->waiting_conns_lock);

    connarray_dealloc(self->fd_to_conn);

}

static void foo_kv_server_tp_dealloc(foo_kv_server *self) {

    #if _FOO_KV_DEBUG == 1
    log_warning("five_one_one_kv.server is being deallocated!!!");
    #endif

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
    sprintf(debug_buff, "got past arg parsing: got port: %d, num_threads: %d", port, num_threads);
    log_debug(debug_buff);
    #endif

    if (num_threads < 2 || num_threads > 16) {
        return -1;
    }
    self->num_threads = num_threads;

    self->storage = PyDict_New();
    if (!self->storage) {
        return -1;
    }
    self->storage_lock = PyMem_RawCalloc(1, sizeof(sem_t));
    if (!self->storage_lock) {
        return -1;
    }
    if (sem_init(self->storage_lock, 0, 1)) {
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
    self->waiting_conns = intq_new();
    if (!self->waiting_conns) {
        return -1;
    }
    self->waiting_conns_lock = PyMem_RawCalloc(1, sizeof(sem_t));
    if (!self->waiting_conns_lock) {
        return -1;
    }
    if (sem_init(self->waiting_conns_lock, 0, 1)) {
        return -1;
    }
    self->waiting_conns_ready_cond = cond_new();
    if (!self->waiting_conns_ready_cond) {
        return -1;
    }

    // return value for io operations
    int rv;

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

    rv = bind(fd, (const struct sockaddr *)&addr, sizeof(addr));
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

    self->fd_to_conn = PyMem_RawCalloc(1, sizeof(struct conn_array_t));
    if (connarray_init(self->fd_to_conn, 8)) {
        log_error("PyMem_RawCalloc() failed to allocate memory");
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

    if (ensure_py_deps()) {
        return NULL;
    }

    return dumps_as_pyobject(args[0]);

}

static PyObject *foo_kv_function_dumps_hashable(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "wrong number of arguments to `dumps_hashable`, expects 1.");
        return NULL;
    }

    if (ensure_py_deps()) {
        return NULL;
    }

    return _dumps_hashable_as_pyobject(args[0]);

}

static PyObject *foo_kv_function_loads(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "wrong number of arguments to `loads`, expects 1.");
        return NULL;
    }

    if (ensure_py_deps()) {
        return NULL;
    }

    return loads_from_pyobject(args[0]);

}

static PyObject *foo_kv_function_loads_hashable(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 1) {
        PyErr_SetString(PyExc_TypeError, "wrong number of arguments to `loads_hashable`, expects 1.");
        return NULL;
    }

    if (ensure_py_deps()) {
        return NULL;
    }

    return _loads_hashable_from_pyobject(args[0]);

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
// following 
static PyObject *foo_kv_server_tp_method_poll_loop(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 0) {
        PyErr_SetString(PyExc_TypeError, "poll_loop() expects no arguments.");
        return NULL;
    }

    poll_loop((foo_kv_server *)self);

    Py_RETURN_NONE;

}

static PyObject *foo_kv_server_tp_method_io_loop(PyObject *self, PyObject *const *args, Py_ssize_t nargs) {

    if (nargs != 0) {
        PyErr_SetString(PyExc_TypeError, "io_loop() expects no arguments.");
        return NULL;
    }

    io_loop((foo_kv_server *)self);

    Py_RETURN_NONE;

}

// helper methods
static void *poll_loop(foo_kv_server *kv_self) {

    struct conn_array_t *fd_to_conn = kv_self->fd_to_conn;

    nfds_t poll_args_size = 0;
    struct pollfd *poll_args = (struct pollfd *)PyMem_RawCalloc(1, sizeof(struct pollfd));

    int32_t has_lock;
    // keep track of number of active connections
    #if _FOO_KV_POLL_DEBUG == 1
    int32_t max_io_workers = kv_self->num_threads - 1;
    #endif
    int32_t num_active = 0;
    int32_t poll_timeout = 0;
    // return value for poll
    int rv;

    #if _FOO_KV_POLL_DEBUG == 1
    char debug_buff[256];
    log_debug("poll_loop(): got past initialize variables");
    #endif

    // the event loop
    while (1) {

        #if _FOO_KV_POLL_DEBUG == 1
        log_debug("poll_loop(): beginning of poll_loop()");
        #endif
        num_active = 0;

        PyMem_RawFree(poll_args); 
        poll_args = PyMem_RawCalloc(fd_to_conn->size + 1, sizeof(struct pollfd));
        poll_args_size = 0;

        // for convenience, the listening fd is put in the first position
        struct pollfd pfd = {kv_self->fd, POLLIN, 0};
        poll_args[poll_args_size] = pfd;
        poll_args_size++;

        // connection fds
        for (int32_t ix = 0; ix < fd_to_conn->maxsize; ix++) {
            struct conn_t *conn = fd_to_conn->arr[ix];
            if (!conn) {
                continue;
            }
            #if _FOO_KV_POLL_DEBUG == 1
            sprintf(debug_buff, "poll_loop(): conn_fd: %d: got non-null conn", conn->fd);
            log_debug(debug_buff);
            #endif
            int events = 0;
            switch (conn->state) {
                case STATE_REQ:
                case STATE_RES:
                case STATE_DISPATCH:
                    #if _FOO_KV_POLL_DEBUG == 1
                    sprintf(debug_buff, "poll_loop(): conn_fd: %d: found connection with active request", conn->fd);
                    log_debug(debug_buff);
                    #endif
                    num_active++;
                    continue;
                case STATE_END:
                    #if _FOO_KV_POLL_DEBUG == 1
                    sprintf(debug_buff, "poll_loop(): conn_fd: %d: found ended connection but it is not flagged for termination", conn->fd);
                    log_debug(debug_buff);
                    #endif
                    continue;
                case STATE_TERM:
                    #if _FOO_KV_POLL_DEBUG == 1
                    sprintf(debug_buff, "poll_loop(): conn_fd: %d: found connection flagged for termination", conn->fd);
                    log_debug(debug_buff);
                    #endif
                    connarray_remove(fd_to_conn, conn);
                    continue;
                case STATE_REQ_WAITING:
                    #if _FOO_KV_POLL_DEBUG == 1
                    sprintf(debug_buff, "poll_loop(): conn_fd: %d: connection waiting for incoming data", conn->fd);
                    log_debug(debug_buff);
                    #endif
                    events = POLLIN;
                    break;
                case STATE_RES_WAITING:
                    #if _FOO_KV_POLL_DEBUG == 1
                    sprintf(debug_buff, "poll_loop(): conn_fd: %d: connection waiting for outgoing data", conn->fd);
                    log_debug(debug_buff);
                    #endif
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

        #if _FOO_KV_POLL_DEBUG == 1
        sprintf(debug_buff, "poll_loop(): got %d/%d active io workers, %d waiting connections, %d total connections",
                num_active, max_io_workers, (int)poll_args_size - 1, fd_to_conn->size);
        log_debug(debug_buff);
        #endif

        // poll for active fds
        poll_timeout = 1000;
        #if _FOO_KV_POLL_DEBUG
        sprintf(debug_buff, "poll_loop: about to call poll(timeout=%d)", poll_timeout);
        log_debug(debug_buff);
        #endif
        Py_BEGIN_ALLOW_THREADS
        rv = poll(poll_args, poll_args_size, poll_timeout);
        Py_END_ALLOW_THREADS
        if (rv < 0) {
            PyErr_SetString(PyExc_RuntimeError, "poll()");
            return NULL;
        }

        #if _FOO_KV_POLL_DEBUG == 1
        log_debug("poll_loop(): called poll()");
        #endif

        // process active connections
        // TODO comments
        for (nfds_t ix = 1; ix < poll_args_size; ix++) {
            if (!poll_args[ix].revents) {
                continue;
            }
            struct conn_t *conn = fd_to_conn->arr[poll_args[ix].fd];
            if (!conn) {
                log_error("poll_loop(): connection object of active fd became null");
                continue;
            }
            has_lock = sem_trywait(conn->lock);
            if (has_lock < 0) {
                if (errno == EINVAL) {
                    log_error("poll_loop(): sem_trywait() failed");
                    return NULL;
                }
                log_warning("poll_loop(): conn lock is locked for idle connection");
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
            if (sem_post(conn->lock)) {
                log_error("poll_loop(): sem_post() failed");
                return NULL;
            }
            if (is_waiting) {
                #if _FOO_KV_POLL_DEBUG == 1
                sprintf(debug_buff, "poll_loop(): conn_fd: %d: about to acquire waiting_conns_lock", conn->fd);
                log_debug(debug_buff);
                #endif
                if (threadsafe_sem_wait(kv_self->waiting_conns_lock)) {
                    log_error("poll_loop(): sem_wait() failed");
                    return NULL;
                }
                #if _FOO_KV_POLL_DEBUG == 1
                sprintf(debug_buff, "poll_loop(): conn_fd: %d: successfully acquired waiting_conns_lock", conn->fd);
                log_debug(debug_buff);
                #endif
                if (intq_put(kv_self->waiting_conns, conn->fd)) {
                    log_error("poll_loop(): failed to enqueue connection");
                    return NULL;
                }

                if (sem_post(kv_self->waiting_conns_lock)) {
                    log_error("poll_loop(): sem_post() failed");
                    return NULL;
                }
                #if _FOO_KV_POLL_DEBUG == 1
                sprintf(debug_buff, "poll_loop(): conn_fd: %d: released waiting_conns_lock", conn->fd);
                log_debug(debug_buff);
                #endif

                // notify other threads that a connection is ready
                if (cond_notify(kv_self->waiting_conns_ready_cond)) {
                    log_error("poll_loop(): cond_notify() failed.");
                    return NULL;
                }

                #if _FOO_KV_POLL_DEBUG == 1
                sprintf(debug_buff, "poll_loop(): conn_fd: %d: notified waiting_conns_ready_cond", conn->fd);
                log_debug(debug_buff);
                #endif

            } // end of is waiting block

        } // end of loop

        int32_t is_queue_empty = 0;

        // give another notify, just to be safe
        #if _FOO_KV_POLL_DEBUG == 1
        sprintf(debug_buff, "poll_loop(): check waiting conns queue: about to acquire waiting_conns_lock");
        log_debug(debug_buff);
        #endif
        if (threadsafe_sem_wait(kv_self->waiting_conns_lock)) {
            log_error("poll_loop(): sem_wait() failed");
            return NULL;
        }
        #if _FOO_KV_POLL_DEBUG == 1
        sprintf(debug_buff, "poll_loop(): check waiting conns queue: successfully acquired waiting_conns_lock");
        log_debug(debug_buff);
        #endif
        is_queue_empty = intq_empty(kv_self->waiting_conns);

        if (sem_post(kv_self->waiting_conns_lock)) {
            log_error("poll_loop(): sem_post() failed");
            return NULL;
        }
        #if _FOO_KV_POLL_DEBUG == 1
        sprintf(debug_buff, "poll_loop(): check waiting conns queue: released waiting_conns_lock");
        log_debug(debug_buff);
        #endif

        // notify other threads that a connection is ready
        if (!is_queue_empty) {
            #if _FOO_KV_POLL_DEBUG == 1
            sprintf(debug_buff, "poll_loop(): check waiting conns queue: queue is not empty, notifying");
            log_debug(debug_buff);
            #endif
            if (cond_notify(kv_self->waiting_conns_ready_cond)) {
                log_error("poll_loop(): cond_notify() failed.");
                return NULL;
            }
        } else {
            #if _FOO_KV_POLL_DEBUG == 1
            sprintf(debug_buff, "poll_loop(): check waiting conns queue: queue is empty, not notifying");
            log_debug(debug_buff);
            #endif
        }

        #if _FOO_KV_POLL_DEBUG == 1
        log_debug("poll_loop(): about to accept new connections");
        #endif

        // accept new connections
        if (poll_args[0].revents) {
            accept_new_conn(fd_to_conn, kv_self->fd);
        }

        #if _FOO_KV_POLL_DEBUG == 1
        log_debug("poll_loop(): end of loop");
        #endif

    }

    return NULL;

}

static void *io_loop(foo_kv_server *kv_self) {

    struct conn_array_t *fd_to_conn = kv_self->fd_to_conn;

    #if _FOO_KV_IO_DEBUG == 1
    char debug_buff[256];
    log_debug("io_loop(): got past initialization");
    #endif

    while (1) {

        #if _FOO_KV_IO_DEBUG == 1
        log_debug("io_loop(): beginning of loop");
        #endif

        // need to acquire lock every time we do a waiting_conns operation
        #if _FOO_KV_IO_DEBUG == 1
        log_debug("io_loop(): about to acquire waiting_conns_lock");
        #endif
        if (threadsafe_sem_wait(kv_self->waiting_conns_lock)) {
            log_error("io_loop(): sem_wait() failed");
            return NULL;
        }
        #if _FOO_KV_IO_DEBUG == 1
        log_debug("io_loop(): successfully acquired waiting_conns_lock");
        #endif
        // check if deq still has len
        if (intq_empty(kv_self->waiting_conns)) {

            // we might have to wait a while
            // first, release lock
            if (sem_post(kv_self->waiting_conns_lock)) {
                log_error("io_loop(): sem_post() failed");
                return NULL;
            }
            #if _FOO_KV_IO_DEBUG == 1
            log_debug("io_loop(): released waiting_conns_lock");
            #endif

            // waiting_conns_ready_cond will be released when there are items on the q
            #if _FOO_KV_IO_DEBUG == 1
            log_debug("io_loop(): about to wait for waiting_conns_ready_cond");
            #endif
            if (cond_wait(kv_self->waiting_conns_ready_cond)) {
                log_error("io_loop(): cond_wait() failed");
                return NULL;
            }

            #if _FOO_KV_IO_DEBUG == 1
            log_debug("io_loop(): got notified by waiting_conns_ready_cond");
            #endif

            #if _FOO_KV_IO_DEBUG == 1
            log_debug("io_loop(): about to acquire waiting_conns_lock");
            #endif
            if (threadsafe_sem_wait(kv_self->waiting_conns_lock)) {
                log_error("io_loop(): sem_wait() failed");
                return NULL;
            }
            #if _FOO_KV_IO_DEBUG == 1
            log_debug("io_loop(): successfully acquired waiting_conns_lock");
            #endif
            // check if deq still has len
            if (intq_empty(kv_self->waiting_conns)) {
                // whoops, another thread got here first, reiterate
                if (sem_post(kv_self->waiting_conns_lock)) {
                    log_error("io_loop(): sem_post() failed");
                    return NULL;
                }
                #if _FOO_KV_IO_DEBUG == 1
                log_debug("io_loop(): released waiting_conns_cond");
                log_debug("io_loop(): found empty deque after getting waiting conns notification");
                #endif
                continue;
            }

        } // end if 0 len

        // if we get here, we should always have the waiting conns lock
        int32_t conn_fd = intq_get(kv_self->waiting_conns);
        if (sem_post(kv_self->waiting_conns_lock)) {
            log_error("io_loop(): sem_post() failed");
            return NULL;
        }
        #if _FOO_KV_IO_DEBUG == 1
        log_debug("io_loop(): released waiting_conns_cond");
        #endif
        if (conn_fd < 0) {
            log_error("io_loop(): failed get from deque: this should never happen if the empty checks and locks worked as intended");
            return NULL;
        }
        struct conn_t *conn = fd_to_conn->arr[conn_fd];
        if (!conn) {
            #if _FOO_KV_IO_DEBUG == 1
            sprintf(debug_buff, "io_loop(): conn_fd: %d: pop from waiting conns queue corresponds to null connection, perhaps this is expected", conn_fd);
            log_debug(debug_buff);
            #endif
            continue;
        }
        if (connection_io(kv_self, conn) < 0) {
            if (PyErr_Occurred()) {
                #if _FOO_KV_IO_DEBUG == 1
                sprintf(debug_buff, "io_loop(): conn_fd: %d: connection_io() reported py error", conn_fd);
                log_error(debug_buff);
                #else
                log_error("io_loop(): connection_io() reported py error");
                #endif
                return NULL;
            }
            #if _FOO_KV_IO_DEBUG == 1
            sprintf(debug_buff, "io_loop(): conn_fd: %d: connection io returned error", conn_fd);
            log_error(debug_buff);
            #else
            log_error("io_loop(): connection io returned error");
            #endif
        } // end connection_io() error check

        if (conn->state == STATE_END) {
            conn->state = STATE_TERM;
        }

    } // end primary loop

    return NULL;

}

// server public methods
static PyMethodDef foo_kv_server_tp_methods[] = {
    {"poll_loop", _PyCFunction_CAST(foo_kv_server_tp_method_poll_loop), METH_FASTCALL, "Start the server operations."},
    {"io_loop", _PyCFunction_CAST(foo_kv_server_tp_method_io_loop), METH_FASTCALL, "Start the server operations."},
    {NULL, NULL, 0, NULL}
};

// define our members
static PyMemberDef foo_kv_server_tp_members[] = {
    {"num_threads", T_INT, offsetof(foo_kv_server, num_threads), READONLY, ""},
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
    {"dumps_hashable", _PyCFunction_CAST(foo_kv_function_dumps_hashable), METH_FASTCALL, "Serialize user data, enforces hashable."},
    {"loads", _PyCFunction_CAST(foo_kv_function_loads), METH_FASTCALL, "Deserialize user data."},
    {"loads_hashable", _PyCFunction_CAST(foo_kv_function_loads_hashable), METH_FASTCALL, "Deserialize user data, enforces hashable."},
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

    // create module
    PyObject *foo_kv_module = PyModule_Create(&foo_kv_module_def);

    // add server type
    PyModule_AddType(foo_kv_module, &FooKVServerType);

    // add response constants
    PyModule_AddIntConstant(foo_kv_module, "RES_OK", RES_OK);
    PyModule_AddIntConstant(foo_kv_module, "RES_UNKNOWN", RES_UNKNOWN);
    PyModule_AddIntConstant(foo_kv_module, "RES_ERR_SERVER", RES_ERR_SERVER);
    PyModule_AddIntConstant(foo_kv_module, "RES_ERR_CLIENT", RES_ERR_CLIENT);
    PyModule_AddIntConstant(foo_kv_module, "RES_BAD_CMD", RES_BAD_CMD);
    PyModule_AddIntConstant(foo_kv_module, "RES_BAD_TYPE", RES_BAD_TYPE);
    PyModule_AddIntConstant(foo_kv_module, "RES_BAD_KEY", RES_BAD_KEY);
    PyModule_AddIntConstant(foo_kv_module, "RES_BAD_ARGS", RES_BAD_ARGS);
    PyModule_AddIntConstant(foo_kv_module, "RES_BAD_OP", RES_BAD_OP);
    PyModule_AddIntConstant(foo_kv_module, "RES_BAD_IX", RES_BAD_IX);
    PyModule_AddIntConstant(foo_kv_module, "RES_BAD_HASH", RES_BAD_HASH);

    // add other constants
    PyModule_AddIntConstant(foo_kv_module, "MAX_MSG_SIZE", MAX_MSG_SIZE);
    PyModule_AddIntConstant(foo_kv_module, "MAX_KEY_SIZE", MAX_KEY_SIZE);
    PyModule_AddIntConstant(foo_kv_module, "MAX_VAL_SIZE", MAX_VAL_SIZE);

    return foo_kv_module;

}
