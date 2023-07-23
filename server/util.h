#ifndef _FOO_KV_UTIL
#define _FOO_KV_UTIL

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <Python.h>

// CHANGE ME
#define _FOO_KV_DEBUG 1

#define DEFAULT_MSG_SIZE 4096
#define MAX_MSG_SIZE 65536
#define MAX_KEY_SIZE 1024
#define MAX_VAL_SIZE MAX_MSG_SIZE - 2048
#define CEIL(x, y) ((y) * ((x) / (y) + ((x) % (y) != 0)))

// cached python objects
extern int32_t is_py_deps_init;
extern PyObject *_builtins;
extern PyObject *_asyncio_module;
extern PyObject *_queue_class;
extern PyObject *_put_nowait_str;
extern PyObject *_logging_module;
extern PyObject *_logger;
extern PyObject *_error_str;
extern PyObject *_warning_str;
extern PyObject *_info_str;
extern PyObject *_debug_str;
extern PyObject *_collections_module;
extern PyObject *_deq_class;
extern PyObject *_pop_str;
extern PyObject *_push_str;
extern PyObject *_json_module;
extern PyObject *_threading_module;
extern PyObject *_threading_lock;
extern PyObject *_threading_cond;
extern PyObject *_threading_sem;
extern PyObject *_acquire_str;
extern PyObject *_release_str;
extern PyObject *_locked_str;
extern PyObject *_notify_str;
extern PyObject *_wait_str;
extern PyObject *_acquire_kwargs_noblock;
extern PyObject *_acquire_kwargs_timeout;
extern PyObject *_acquire_kwargs_block;
extern PyObject *_type_f;
extern PyObject *_items;
extern PyObject *_type_to_symbol;
extern PyObject *_string_class;
extern PyObject *_int_symbol;
extern PyObject *_float_symbol;
extern PyObject *_bytes_symbol;
extern PyObject *_string_symbol;
extern PyObject *_list_symbol;
extern PyObject *_bool_symbol;
extern PyObject *_json_kwargs;
extern PyObject *_json_loads_f;
extern PyObject *_json_dumps_f;
extern PyObject *_decode_str;
extern PyObject *_utf8_str;
extern PyObject *_empty_args;

// this sets the above cached objects
int32_t ensure_py_deps();
// threading utils
int32_t _threading_lock_acquire(PyObject *lock);
int32_t _threading_lock_acquire_block(PyObject *lock);
int32_t _threading_lock_release(PyObject *lock);
int32_t _threading_lock_locked(PyObject *lock);

// server connection states
enum {
    STATE_REQ = 0,
    STATE_RES = 1,
    STATE_DISPATCH = 2,
    STATE_REQ_WAITING = 3,
    STATE_RES_WAITING = 4,
    STATE_END = 5,
};

struct Conn {
    int fd;
    uint32_t state;
    size_t rbuff_size;
    size_t rbuff_max;
    uint8_t *rbuff;
    size_t wbuff_size;
    size_t wbuff_sent;
    size_t wbuff_max;
    uint8_t *wbuff;
    int32_t connid;
    PyObject *lock;

};

struct Response {
    int32_t status;
    uint8_t *data;
    int32_t datalen;
};

enum {
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2,
    RES_UNKNOWN = 3,
};

// basic utils
void log_error(const char *msg);
void log_warning(const char *msg);
void log_info(const char *msg);
void log_debug(const char *msg);
void die(const char *msg);
int32_t randint(int32_t min, int32_t max);
int32_t hash_given_len(const uint8_t *s, size_t n);
int32_t hash(const uint8_t *s);

// read/write
int32_t read_full(int fd, char *buff, size_t n); 
int32_t write_all(int fd, const char *buff, size_t n);

// connection management
struct Conn *conn_new(int connfd);
int32_t conn_resize_rbuff(struct Conn *conn, uint32_t newsize);
int32_t conn_resize_wbuff(struct Conn *conn, uint32_t newsize);
int32_t conn_flush(struct Conn *conn, uint32_t flushsize);
int32_t conn_write_response(struct Conn *conn, const struct Response *response);

#endif
