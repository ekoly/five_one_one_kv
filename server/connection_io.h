#ifndef _FOO_SERVER_H
#define _FOO_SERVER_H

#include <semaphore.h>
#include <Python.h>

#include "util.h"
#include "ttl.h"
#include "pythontypes.h"

// connection utilities
int32_t connection_io(foo_kv_server *server, struct conn_t *conn);
int32_t state_req(struct conn_t *conn);
int32_t try_fill_buffer(struct conn_t *conn);
int32_t state_dispatch(foo_kv_server *server, struct conn_t *conn);
int32_t try_one_request(struct conn_t *conn);
int32_t state_res(struct conn_t *conn);
int32_t try_flush_buffer(struct conn_t *conn);

#endif
