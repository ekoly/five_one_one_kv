#ifndef _FOO_SERVER_H
#define _FOO_SERVER_H

#include <semaphore.h>
#include <Python.h>

#include "util.h"

struct conn_array_t {
    int32_t size;
    int32_t maxsize;
    struct conn_t **arr;
};

// define our python type
typedef struct foo_kv_server {
    PyObject_HEAD
    PyObject *storage;
    sem_t *storage_lock;
    PyObject *user_locks;
    PyObject *user_locks_lock;
    int fd;
    struct conn_array_t *fd_to_conn;
    struct intq_t *waiting_conns;
    sem_t *waiting_conns_lock;
    struct cond_t *waiting_conns_ready_cond;
    int num_threads;
} foo_kv_server;

// generic utilities
void fd_set_nb(int fd);

// connection array utility functions
int32_t connarray_init(struct conn_array_t *fd_to_conn, int maxsize);
int32_t connarray_dealloc(struct conn_array_t *conns);
int32_t connarray_put(struct conn_array_t *fd_to_conn, struct conn_t *conn);
int32_t connarray_remove(struct conn_array_t *fd_to_conn, struct conn_t *conn);

// connection utilities
int32_t accept_new_conn(struct conn_array_t *fd_to_conn, int fd);
int32_t connection_io(foo_kv_server *server, struct conn_t *conn);
int32_t state_req(struct conn_t *conn);
int32_t try_fill_buffer(struct conn_t *conn);
int32_t state_dispatch(foo_kv_server *server, struct conn_t *conn);
int32_t try_one_request(struct conn_t *conn);
int32_t state_res(struct conn_t *conn);
int32_t try_flush_buffer(struct conn_t *conn);

#endif
