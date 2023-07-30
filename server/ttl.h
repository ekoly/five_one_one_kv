#include <time.h>
#include <stdint.h>

#include <Python.h>

#ifndef _FOO_KV_TTL
#define _FOO_KV_TTL

#include "util.h"
#include "pythontypes.h"

#define TTL_HEAP_DEFAULT_SIZE 4096

// allocation method declarations
PyObject *foo_kv_ttl_tp_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs);
void foo_kv_ttl_tp_clear(foo_kv_ttl *self);
void foo_kv_ttl_tp_dealloc(foo_kv_ttl *self);
int foo_kv_ttl_tp_init(foo_kv_ttl *self, PyObject *args, PyObject *kwargs);

foo_kv_ttl *foo_kv_ttl_new(PyObject *key, time_t seconds);

struct ttl_heap_t *ttl_heap_new();
void ttl_heap_dealloc(struct ttl_heap_t *ttl_heap);
#define ttl_heap_empty(ttl_heap) ((ttl_heap->front) >= (ttl_heap->back))
#define ttl_heap_full(ttl_heap) ((ttl_heap->back) >= (ttl_heap->max))
foo_kv_ttl *ttl_heap_peek(struct ttl_heap_t *ttl_heap);
foo_kv_ttl *ttl_heap_get(struct ttl_heap_t *ttl_heap);
int32_t ttl_heap_put(struct ttl_heap_t *ttl_heap, foo_kv_ttl *item);

// allocation method declarations
PyObject *foo_kv_ttl_heap_tp_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs);
void foo_kv_ttl_heap_tp_clear(foo_kv_ttl_heap *self);
void foo_kv_ttl_heap_tp_dealloc(foo_kv_ttl_heap *self);
int foo_kv_ttl_heap_tp_init(foo_kv_ttl_heap *self, PyObject *args, PyObject *kwargs);

int32_t foo_kv_ttl_heap_put(foo_kv_ttl_heap *self, PyObject *key, time_t ttl);
int32_t foo_kv_ttl_heap_put_dt(foo_kv_ttl_heap *self, PyObject *key, PyObject *ttl);
PyObject *foo_kv_ttl_heap_get(foo_kv_ttl_heap *self);
int32_t foo_kv_ttl_heap_invalidate(foo_kv_ttl_heap *self, PyObject *key);

foo_kv_ttl_heap *foo_kv_ttl_heap_new();

int32_t _ttl_heap_siftdown(struct ttl_heap_t *ttl_heap, int32_t item_ix);
int32_t _ttl_heap_siftup(struct ttl_heap_t *ttl_heap, int32_t item_ix);
int32_t _ttl_heap_is_valid(struct ttl_heap_t *ttl_heap);

#endif
