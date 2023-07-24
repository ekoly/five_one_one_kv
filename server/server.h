#ifndef _FOO_SERVER_H
#define _FOO_SERVER_H

#include <semaphore.h>
#include <Python.h>

#include "util.h"

struct ConnArray {
    int32_t size;
    int32_t maxsize;
    struct Conn **arr;
};

// define our python type
typedef struct foo_kv_server {
    PyObject_HEAD
    PyObject *storage;
    PyObject *storage_lock;
    PyObject *user_locks;
    PyObject *user_locks_lock;
    int fd;
    struct ConnArray *fd_to_conn;
    struct intq_t *waiting_conns;
    sem_t *waiting_conns_lock;
    struct cond_t *waiting_conns_ready_cond;
    sem_t *io_conns_sem; // keeps track of # of active requests
    int num_threads;
} foo_kv_server;

// generic utilities
void fd_set_nb(int fd);

// connection array utility functions
int32_t connarray_init(struct ConnArray *fd_to_conn, int maxsize);
int32_t connarray_dealloc(struct ConnArray *conns);
int32_t connarray_put(struct ConnArray *fd_to_conn, struct Conn *conn);
int32_t connarray_remove(struct ConnArray *fd_to_conn, struct Conn *conn);

// connection utilities
int32_t accept_new_conn(struct ConnArray *fd_to_conn, int fd);
int32_t connection_io(foo_kv_server *server, struct Conn *conn);
int32_t state_req(struct Conn *conn);
int32_t try_fill_buffer(struct Conn *conn);
int32_t state_dispatch(foo_kv_server *server, struct Conn *conn);
int32_t try_one_request(struct Conn *conn);
int32_t state_res(struct Conn *conn);
int32_t try_flush_buffer(struct Conn *conn);

#endif
