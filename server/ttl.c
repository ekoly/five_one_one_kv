// timeout functionality lives here
#include "util.h"
#include "ttl.h"

// CHANGE ME
#define _FOO_KV_DEBUG 1

// server py class
PyTypeObject FooKVTTLType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "server",                                   /*tp_name*/
    sizeof(foo_kv_ttl),                         /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)foo_kv_ttl_tp_dealloc,          /*tp_dealloc*/
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
    (inquiry)foo_kv_ttl_tp_clear,               /*tp_clear*/
    0,                                          /*tp_richcompare*/
    0,                                          /*tp_weaklistoffset*/
    0,                                          /*tp_iter*/
    0,                                          /*tp_iternext*/
    0,                                          /*tp_methods*/
    0,                                          /*tp_members*/
    0,                                          /*tp_getsets*/
    0,                                          /*tp_base*/
    0,                                          /*tp_dict*/
    0,                                          /*tp_descr_get*/
    0,                                          /*tp_descr_set*/
    0,                                          /*tp_dictoffset*/
    (initproc)foo_kv_ttl_tp_init,               /*tp_init*/
    0,                                          /*tp_alloc*/
    foo_kv_ttl_tp_new,                          /*tp_new*/
};

// server py class
PyTypeObject FooKVTTLHeapType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "server",                                   /*tp_name*/
    sizeof(foo_kv_ttl_heap),                    /*tp_basicsize*/
    0,                                          /*tp_itemsize*/
    (destructor)foo_kv_ttl_heap_tp_dealloc,     /*tp_dealloc*/
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
    (inquiry)foo_kv_ttl_heap_tp_clear,          /*tp_clear*/
    0,                                          /*tp_richcompare*/
    0,                                          /*tp_weaklistoffset*/
    0,                                          /*tp_iter*/
    0,                                          /*tp_iternext*/
    0,                                          /*tp_methods*/
    0,                                          /*tp_members*/
    0,                                          /*tp_getsets*/
    0,                                          /*tp_base*/
    0,                                          /*tp_dict*/
    0,                                          /*tp_descr_get*/
    0,                                          /*tp_descr_set*/
    0,                                          /*tp_dictoffset*/
    (initproc)foo_kv_ttl_heap_tp_init,          /*tp_init*/
    0,                                          /*tp_alloc*/
    foo_kv_ttl_heap_tp_new,                     /*tp_new*/
};

// allocation method declarations
PyObject *foo_kv_ttl_tp_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs) {

    foo_kv_ttl *self = (foo_kv_ttl *)subtype->tp_alloc(subtype, 0);

    return (PyObject *)self;

}

void foo_kv_ttl_tp_clear(foo_kv_ttl *self) {
    Py_XDECREF(self->key);
}

void foo_kv_ttl_tp_dealloc(foo_kv_ttl *self) {
    foo_kv_ttl_tp_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

int32_t foo_kv_ttl_tp_init(foo_kv_ttl *self, PyObject *args, PyObject *kwargs) {
    return 0;
}

foo_kv_ttl *foo_kv_ttl_new(PyObject *key, time_t seconds) {

    PyObject *py_result = (PyObject *)PyObject_New(foo_kv_ttl, &FooKVTTLType);
    foo_kv_ttl *result = (foo_kv_ttl *)py_result;
    result->ttl = seconds;
    Py_INCREF(key);
    result->key = key;
    result->is_valid = 1;

    return result;

}

struct ttl_heap_t *ttl_heap_new() {

    struct ttl_heap_t *ttl_heap = PyMem_RawCalloc(1, sizeof(struct ttl_heap_t));
    if (!ttl_heap) {
        return NULL;
    }
    ttl_heap->heap = PyMem_RawCalloc(TTL_HEAP_DEFAULT_SIZE, sizeof(foo_kv_ttl *));
    if (!ttl_heap->heap) {
        PyMem_RawFree(ttl_heap);
        return NULL;
    }
    ttl_heap->front = 0;
    ttl_heap->back = 0;
    ttl_heap->max = TTL_HEAP_DEFAULT_SIZE;

    return ttl_heap;

}

void ttl_heap_dealloc(struct ttl_heap_t *ttl_heap) {
    PyMem_RawFree(ttl_heap->heap);
    PyMem_RawFree(ttl_heap);
}


foo_kv_ttl *ttl_heap_peek(struct ttl_heap_t *ttl_heap) {

    if (ttl_heap_empty(ttl_heap)) {
        return NULL;
    }

    return ttl_heap->heap[ttl_heap->front];

}

foo_kv_ttl *ttl_heap_get(struct ttl_heap_t *ttl_heap) {

    #if _FOO_KV_DEBUG == 1
    log_debug("ttl_heap_get(): starting");
    #endif

    if (ttl_heap_empty(ttl_heap)) {
        return NULL;
    }

    foo_kv_ttl *result, *back_item;
    int32_t front_ix, back_ix;

    front_ix = ttl_heap->front;
    result = ttl_heap->heap[front_ix];
    // check if empty, in which case we return immediately
    if (ttl_heap_empty(ttl_heap)) {
        ttl_heap->front = 0;
        ttl_heap->back = 0;
        return result;
    }
    back_ix = ttl_heap->back - 1;
    back_item = ttl_heap->heap[back_ix];
    Py_INCREF(back_item);
    ttl_heap->back--;
    ttl_heap->heap[front_ix] = back_item;

    #if _FOO_KV_DEBUG == 1
    log_debug("ttl_heap_get(): got item: about to sift up");
    #endif

    if (_ttl_heap_siftup(ttl_heap, front_ix) < 0) {
        Py_DECREF(result);
        Py_DECREF(back_item);
        return NULL;
    }
    Py_DECREF(back_item);

    #if _FOO_KV_DEBUG == 1
    log_debug("ttl_heap_get(): finished");
    #endif

    return result;

}

int32_t ttl_heap_put(struct ttl_heap_t *ttl_heap, foo_kv_ttl *item) {

    Py_INCREF(item);
    int32_t back_ix, final_ix;
    back_ix = ttl_heap->back;

    if (ttl_heap_full(ttl_heap)) {
        int32_t currsize = back_ix - ttl_heap->front;
        int32_t newsize = currsize + 1;
        newsize = CEIL(newsize, TTL_HEAP_DEFAULT_SIZE);
        foo_kv_ttl **newheap = PyMem_RawCalloc(newsize, sizeof(foo_kv_ttl *));
        if (!newheap) {
            Py_DECREF(item);
            return -1;
        }
        memcpy(newheap, ttl_heap->heap + ttl_heap->front, currsize * sizeof(foo_kv_ttl *));
        PyMem_RawFree(ttl_heap->heap);
        ttl_heap->heap = newheap;
        ttl_heap->front = 0;
        ttl_heap->back = currsize;
        ttl_heap->max = newsize;
    }

    ttl_heap->heap[back_ix] = item;
    ttl_heap->back++;

    if ((final_ix = _ttl_heap_siftdown(ttl_heap, back_ix)) < 0) {
        return -1;
    }

    return final_ix;

}

// allocation method declarations
PyObject *foo_kv_ttl_heap_tp_new(PyTypeObject *subtype, PyObject *args, PyObject *kwargs) {

    foo_kv_ttl_heap *self = (foo_kv_ttl_heap *)subtype->tp_alloc(subtype, 0);

    return (PyObject *)self;

}

void foo_kv_ttl_heap_tp_clear(foo_kv_ttl_heap *self) {

    Py_DECREF(self->key_to_ttl);
    ttl_heap_dealloc(self->heap);
    cond_destroy(self->notifier);
    sem_destroy(self->lock);

}

void foo_kv_ttl_heap_tp_dealloc(foo_kv_ttl_heap *self) {

    foo_kv_ttl_heap_tp_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);

}

int32_t foo_kv_ttl_heap_tp_init(foo_kv_ttl_heap *self, PyObject *args, PyObject *kwargs) {

    self->key_to_ttl = PyDict_New();
    if (!self->key_to_ttl) {
        return -1;
    }
    self->heap = ttl_heap_new();
    if (!self->heap) {
        return -1;
    }
    self->notifier = cond_new();
    if (!self->notifier) {
        return -1;
    }
    sem_init(self->lock, 0, 1);

    return 0;

}

int32_t foo_kv_ttl_heap_put(foo_kv_ttl_heap *self, PyObject *key, time_t ttl) {

    #if _FOO_KV_DEBUG == 1
    log_debug("foo_kv_ttl_heap_put(): started");
    #endif

    Py_BEGIN_ALLOW_THREADS
    sem_wait(self->lock);
    Py_END_ALLOW_THREADS

    PyObject *py_ttl = PyDict_GetItem(self->key_to_ttl, key);
    foo_kv_ttl *ttl_item;

    if (!py_ttl) {
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }
        #if _FOO_KV_DEBUG == 1
        log_debug("foo_kv_ttl_heap_put(): no existing ttl for this key");
        #endif
    } else {
        ttl_item = (foo_kv_ttl *)py_ttl;
        ttl_item->is_valid = 0;
        #if _FOO_KV_DEBUG == 1
        log_debug("foo_kv_ttl_heap_put(): invalidated existing ttl for this key");
        #endif
    }

    ttl_item = foo_kv_ttl_new(key, ttl);
    #if _FOO_KV_DEBUG == 1
    log_debug("foo_kv_ttl_heap_put(): created new ttl");
    #endif
    int32_t final_ix = ttl_heap_put(self->heap, ttl_item);
    #if _FOO_KV_DEBUG == 1
    log_debug("foo_kv_ttl_heap_put(): put new ttl on heap");
    #endif
    if (final_ix == 0) {
        cond_notify(self->notifier);
    }
    if (PyDict_SetItem(self->key_to_ttl, key, (PyObject *)ttl_item) < 0) {
        ttl_item->is_valid = 0;
        return -1;
    }

    Py_BEGIN_ALLOW_THREADS
    sem_post(self->lock);
    Py_END_ALLOW_THREADS

    return 0;

}

int32_t foo_kv_ttl_heap_put_dt(foo_kv_ttl_heap *self, PyObject *key, PyObject *ttl) {

    Py_INCREF(_timestamp_str);
    PyObject *py_epoch = PyObject_CallMethodNoArgs(ttl, _timestamp_str);
    Py_DECREF(_timestamp_str);

    if (!py_epoch) {
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }
        return -1;
    }

    double epoch = PyFloat_AsDouble(py_epoch);
    Py_DECREF(py_epoch);

    int32_t result = foo_kv_ttl_heap_put(self, key, (time_t)epoch);

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    PyObject *ks = PyUnicode_FromFormat("%U", key);
    PyObject *dts = PyUnicode_FromFormat("%d", (time_t)epoch);
    if (!ks || !dts) {
        log_debug("storage_ttl_loop(): unable to convert key or ttl to text!");
    } else {
        PyObject *kb = PyUnicode_AsASCIIString(ks);
        PyObject *dtb = PyUnicode_AsASCIIString(dts);
        Py_DECREF(ks);
        Py_DECREF(dts);
        if (result) {
            sprintf(debug_buffer, "ttl_heap_put_dt(): failure: key: %s, ttl: %s", PyBytes_AS_STRING(kb), PyBytes_AS_STRING(dtb));
        } else {
            sprintf(debug_buffer, "ttl_heap_put_dt(): success: key: %s, ttl: %s", PyBytes_AS_STRING(kb), PyBytes_AS_STRING(dtb));
        }
        Py_DECREF(kb);
        Py_DECREF(dtb);
        log_debug(debug_buffer);
    }
    #endif

    return result;

}

PyObject *foo_kv_ttl_heap_get(foo_kv_ttl_heap *self) {

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    #endif
    foo_kv_ttl *next_ttl;
    struct timespec current_timespec;

    // we do this in a loop because the heap can change while we're waiting
    while (1) {

        next_ttl = ttl_heap_peek(self->heap);
        if (!next_ttl) {
            if (cond_wait(self->notifier) < 0) {
                return NULL;
            }
            continue;
        }

        clock_gettime(CLOCK_REALTIME, &current_timespec);
        #if _FOO_KV_DEBUG == 1
        sprintf(debug_buffer, "heap_get(): got gmtime: %ld", current_timespec.tv_sec);
        log_debug(debug_buffer);
        #endif

        if (!next_ttl->is_valid) {

            #if _FOO_KV_DEBUG == 1
            log_debug("heap_get(): got invalidated ttl, deleting");
            #endif

            Py_BEGIN_ALLOW_THREADS
            sem_wait(self->lock);
            Py_END_ALLOW_THREADS

            foo_kv_ttl *invalid_ttl = ttl_heap_get(self->heap);
            if ((PyObject *)invalid_ttl == PyDict_GetItem(self->key_to_ttl, invalid_ttl->key)) {
                _pyobject_safe_delitem(self->key_to_ttl, invalid_ttl->key);
            }
            Py_DECREF(invalid_ttl);

            Py_BEGIN_ALLOW_THREADS
            sem_post(self->lock);
            Py_END_ALLOW_THREADS

            continue;

        } else if (next_ttl->ttl <= current_timespec.tv_sec) {
            #if _FOO_KV_DEBUG == 1
            sprintf(debug_buffer, "heap_get(): got expired ttl: %ld", next_ttl->ttl);
            log_debug(debug_buffer);
            #endif
            break;
        } else {
            #if _FOO_KV_DEBUG == 1
            sprintf(debug_buffer, "heap_get(): got waiting ttl: %ld", next_ttl->ttl);
            log_debug(debug_buffer);
            #endif
            struct timespec ttl_as_timespec;
            ttl_as_timespec.tv_sec = next_ttl->ttl;
            ttl_as_timespec.tv_nsec = 0;
            // the following does not return negative on timeout
            if (cond_timedwait(self->notifier, &ttl_as_timespec) < 0) {
                return NULL;
            }
        }

    }

    Py_BEGIN_ALLOW_THREADS
    sem_wait(self->lock);
    Py_END_ALLOW_THREADS
    // it's possible that the item at the front of the heap is different from the one we got from `peek` earlier
    // In this case we'll address this the next iteration of the loop, it's not a big deal
    next_ttl = ttl_heap_get(self->heap);
    #if _FOO_KV_DEBUG == 1
    if (!_ttl_heap_is_valid(self->heap)) {
        return NULL;
    }
    #endif
    // PyDict_DelItem sometimes segfaults if it looks for a key that is not in the dict
    // But we have a serious problem anyway if the key is not present
    int32_t del_result = _pyobject_safe_delitem(self->key_to_ttl, next_ttl->key);
    if (del_result < 0) {
        #if _FOO_KV_DEBUG == 1
        log_error("ttl_heap_get(): unable to remove ttl from key_to_ttl!");
        #endif
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }
    } else if (del_result == 0) {
        #if _FOO_KV_DEBUG == 1
        log_debug("ttl_heap_get(): expired ttl was not in key_to_ttl, perhaps this is expected.");
        #endif
    }

    #if _FOO_KV_DEBUG == 1
    PyObject *ks = PyUnicode_FromFormat("%U", next_ttl->key);
    if (!ks) {
        log_debug("storage_ttl_loop(): unable to convert expired key to text!");
    } else {
        PyObject *kb = PyUnicode_AsASCIIString(ks);
        Py_DECREF(ks);
        sprintf(debug_buffer, "ttl_heap_get(): got expired key: %s", PyBytes_AS_STRING(kb));
        Py_DECREF(kb);
        log_debug(debug_buffer);
    }
    #endif

    Py_BEGIN_ALLOW_THREADS
    sem_post(self->lock);
    Py_END_ALLOW_THREADS

    Py_DECREF(next_ttl);

    return next_ttl->key;

}

int32_t foo_kv_ttl_heap_invalidate(foo_kv_ttl_heap *self, PyObject *key) {

    Py_BEGIN_ALLOW_THREADS
    sem_wait(self->lock);
    Py_END_ALLOW_THREADS

    // get item returns borrowed reference
    // We will delete and decref it when it gets dequeued
    PyObject *py_ttl = PyDict_GetItem(self->key_to_ttl, key);
    foo_kv_ttl *ttl_item;

    if (!py_ttl) {
        if (PyErr_Occurred()) {
            PyErr_Clear();
        }
        #if _FOO_KV_DEBUG == 1
        log_debug("ttl_heap_invalidate(): got invalidate request that is not in the heap: perhaps this is expected");
        #endif
    } else {
        ttl_item = (foo_kv_ttl *)py_ttl;
        ttl_item->is_valid = 0;
    }

    Py_BEGIN_ALLOW_THREADS
    sem_post(self->lock);
    Py_END_ALLOW_THREADS

    return 0;

}

foo_kv_ttl_heap *foo_kv_ttl_heap_new() {

    foo_kv_ttl_heap *self = (foo_kv_ttl_heap *)PyObject_New(foo_kv_ttl_heap, &FooKVTTLHeapType);

    self->key_to_ttl = PyDict_New();
    if (!self->key_to_ttl) {
        return NULL;
    }
    self->heap = ttl_heap_new();
    if (!self->heap) {
        return NULL;
    }
    self->notifier = cond_new();
    if (!self->notifier) {
        return NULL;
    }
    self->lock = PyMem_RawCalloc(1, sizeof(sem_t));
    sem_init(self->lock, 0, 1);

    return self;

}

int32_t _ttl_heap_siftdown(struct ttl_heap_t *ttl_heap, int32_t item_ix) {

    #if _FOO_KV_DEBUG == 1
    log_debug("ttl_heap_siftdown(): starting");
    #endif

    // I expect that the developer makes sure foo_kv_ttl_heap's lock is held while
    // this method is in progress
    foo_kv_ttl **arr, *item, *parent;
    int32_t front, parent_ix, cmp;
    front = ttl_heap->front;
    arr = ttl_heap->heap + front;
    // normalize item_ix against front
    item_ix -= front;

    item = arr[item_ix];
    // Make our way to the root node (to the left)
    // Move items to the right until we find a place the item at `item_ix` fits
    while (item_ix > 0) {
        parent_ix = (item_ix - 1) >> 1;
        parent = arr[parent_ix];
        Py_INCREF(item);
        Py_INCREF(parent);
        cmp = item->ttl < parent->ttl;
        Py_DECREF(item);
        Py_DECREF(parent);
        if (!cmp) {
            break;
        }
        arr[parent_ix] = item;
        arr[item_ix] = parent;
        item_ix = parent_ix;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("ttl_heap_siftdown(): finished");
    #endif

    return item_ix;

}

int32_t _ttl_heap_siftup(struct ttl_heap_t *ttl_heap, int32_t item_ix) {

    #if _FOO_KV_DEBUG == 1
    log_debug("ttl_heap_siftup(): starting");
    #endif

    // I expect that the developer makes sure foo_kv_ttl_heap's lock is held while
    // this method is in progress
    foo_kv_ttl **arr, *tmp1, *tmp2;
    int32_t front, back_ix, back_parent_ix, child_ix, cmp;
    front = ttl_heap->front;
    arr = ttl_heap->heap + front;
    item_ix -= front;
    back_ix = ttl_heap->back - front;
    back_parent_ix = back_ix >> 1;

    // Make our way to the leftmost childless node
    // Move items to the left until we find a place the item at item_ix fits
    while (item_ix < back_parent_ix) {
        // left child
        child_ix = 2*item_ix + 1;
        if (child_ix + 1 < back_ix) {
            foo_kv_ttl *left, *right;
            left = arr[child_ix];
            right = arr[child_ix + 1];
            Py_INCREF(left);
            Py_INCREF(right);
            cmp = left->ttl < right->ttl;
            Py_DECREF(left);
            Py_DECREF(right);
            child_ix += ((uint32_t)cmp ^ 1);  // increment if cmp is 0
        }
        // move the smaller child to the right
        tmp1 = arr[child_ix];
        tmp2 = arr[item_ix];
        arr[child_ix] = tmp2;
        arr[item_ix] = tmp1;
        item_ix = child_ix;
    }

    #if _FOO_KV_DEBUG == 1
    log_debug("ttl_heap_siftup(): finished");
    #endif

    // remember that item_ix is currently offset- need to un-offset it
    return _ttl_heap_siftdown(ttl_heap, item_ix + front);

}

int32_t _ttl_heap_is_valid(struct ttl_heap_t *ttl_heap) {

    foo_kv_ttl **arr = ttl_heap->heap + ttl_heap->front;
    int32_t back_ix, back_parent_ix;
    back_ix = ttl_heap->back - ttl_heap->front;
    back_parent_ix = back_ix >> 1;

    for (int32_t ix = 0; ix < back_parent_ix; ix++) {
        foo_kv_ttl *parent, *child;
        parent = arr[ix];
        child = arr[ix*2 + 1];
        if (parent->ttl > child->ttl) {
            log_debug("_ttl_heap_is_valid(): left child is out of order");
            return 0;
        }
        if (ix*2 + 2 < back_ix) {
            child = arr[ix*2 + 2];
            if (parent->ttl > child->ttl) {
                log_debug("_ttl_heap_is_valid(): right child is out of order");
                return 0;
            }
        }
    }

    return 1;

}
