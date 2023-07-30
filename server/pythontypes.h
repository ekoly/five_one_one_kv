#ifndef _FOO_KV_PYTHONTYPES
#define _FOO_KV_PYTHONTYPES

#include <semaphore.h>

#include <Python.h>

#include "util.h"

// define our python type
typedef struct foo_kv_ttl {
    PyObject_HEAD
    time_t ttl;
    PyObject *key;
    int32_t is_valid;
} foo_kv_ttl;

struct ttl_heap_t {
    foo_kv_ttl **heap;
    int32_t front;
    int32_t back;
    int32_t max;
};

// define our python type
typedef struct foo_kv_ttl_heap {
    PyObject_HEAD
    PyObject *key_to_ttl;
    struct ttl_heap_t *heap;
    struct cond_t *notifier;
    sem_t *lock;
} foo_kv_ttl_heap;

// define our python type
typedef struct foo_kv_server {
    PyObject_HEAD
    PyObject *storage;
    sem_t *storage_lock;
    PyObject *user_locks;
    PyObject *user_locks_lock;
    foo_kv_ttl_heap *storage_ttl_heap;
    foo_kv_ttl_heap *lock_ttl_heap;
    int fd;
    struct connarray_t *fd_to_conn;
    struct intq_t *waiting_conns;
    sem_t *waiting_conns_lock;
    struct cond_t *waiting_conns_ready_cond;
    int num_threads;
} foo_kv_server;

#endif
