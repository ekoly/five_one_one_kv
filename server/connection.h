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

#ifndef _FOO_KV_CONNECTION
#define _FOO_KV_CONNECTION

// generic utilities
void fd_set_nb(int fd);

// response management
struct response_t {
    int16_t status;
    PyObject *payload;
};

// connection management
struct conn_t {
    int fd;
    int32_t state;
    int32_t err;
    size_t rbuff_size;
    size_t rbuff_read;
    size_t rbuff_max;
    uint8_t *rbuff;
    size_t wbuff_size;
    size_t wbuff_sent;
    size_t wbuff_max;
    uint8_t *wbuff;
    int32_t connid;
    sem_t *lock;

};

struct conn_t *conn_new(int connfd);
int32_t conn_rbuff_resize(struct conn_t *conn, uint32_t newsize);
int32_t conn_wbuff_resize(struct conn_t *conn, uint32_t newsize);
int32_t conn_rbuff_flush(struct conn_t *conn);
int32_t conn_write_response(struct conn_t *conn, const struct response_t *response);

struct connarray_t {
    int32_t size;
    int32_t maxsize;
    struct conn_t **arr;
};

// connection array utility functions
int32_t accept_new_conn(struct connarray_t *fd_to_conn, int fd);
int32_t connarray_init(struct connarray_t *fd_to_conn, int maxsize);
int32_t connarray_dealloc(struct connarray_t *conns);
int32_t connarray_put(struct connarray_t *fd_to_conn, struct conn_t *conn);
int32_t connarray_remove(struct connarray_t *fd_to_conn, struct conn_t *conn);

#endif
