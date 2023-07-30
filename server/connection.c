// home of struct connection_t and struct connarray_t

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


#include "util.h"
#include "connection.h"

// CHANGE ME
#define _FOO_KV_DEBUG 1

// connections utility
int32_t conncounter = 1;


void fd_set_nb(int fd) {

    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0);
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK;

    errno = 0;
    fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
    }

}

struct conn_t *conn_new(int connfd) {

    struct conn_t *conn = PyMem_RawCalloc(1, sizeof(struct conn_t));
    if (conn == NULL) {
        close(connfd);
        return NULL;
    }
    conn->fd = connfd;
    conn->state = STATE_REQ_WAITING;
    conn->err = 0;
    conn->rbuff_size = 0;
    conn->rbuff_read = 0;
    conn->rbuff_max = DEFAULT_MSG_SIZE;
    conn->wbuff_size = 0;
    conn->wbuff_sent = 0;
    conn->wbuff_max = DEFAULT_MSG_SIZE;
    conn->connid = conncounter++;

    conn->rbuff = PyMem_RawCalloc(DEFAULT_MSG_SIZE, sizeof(uint8_t));
    if (!conn->rbuff) {
        return NULL;
    }
    conn->wbuff = PyMem_RawCalloc(DEFAULT_MSG_SIZE, sizeof(uint8_t));
    if (!conn->wbuff) {
        return NULL;
    }

    conn->lock = PyMem_RawCalloc(1, sizeof(sem_t));
    if (!conn->lock) {
        close(connfd);
        return NULL;
    }
    sem_init(conn->lock, 0, 1);

    return conn;

}

int32_t conn_rbuff_resize(struct conn_t *conn, uint32_t newsize) {

    uint8_t *newbuff;

    if (newsize == 0) {
        return -1;
    }

    if (conn->rbuff_read == 0) {
        newbuff = (uint8_t *)PyMem_RawRealloc(conn->rbuff, (newsize + 1) * sizeof(uint8_t));
        if (!newbuff) {
            return -1;
        }
    } else {
        newbuff = PyMem_RawCalloc(newsize + 1, sizeof(uint8_t));
        if (!newbuff) {
            return -1;
        }
        memcpy(newbuff, conn->rbuff + conn->rbuff_read, conn->rbuff_size - conn->rbuff_read);
        PyMem_RawFree(conn->rbuff);
    }

    conn->rbuff = newbuff;
    conn->rbuff_size -= conn->rbuff_read;
    conn->rbuff_read = 0;
    conn->rbuff_max = newsize;

    return 0;

}

int32_t conn_wbuff_resize(struct conn_t *conn, uint32_t newsize) {

    uint8_t *newbuff;

    if (newsize == 0) {
        return -1;
    }

    if (conn->wbuff_sent == 0) {
        newbuff = (uint8_t *)PyMem_RawRealloc(conn->wbuff, (newsize + 1) * sizeof(uint8_t));
        if (!newbuff) {
            return -1;
        }
    } else {
        newbuff = PyMem_RawCalloc(newsize + 1, sizeof(uint8_t));
        if (!newbuff) {
            return -1;
        }
        memcpy(newbuff, conn->wbuff + conn->wbuff_sent, conn->wbuff_size - conn->wbuff_sent);
        PyMem_RawFree(conn->wbuff);
    }

    conn->wbuff = newbuff;
    conn->wbuff_size -= conn->wbuff_sent;
    conn->wbuff_sent = 0;
    conn->wbuff_max = newsize;

    return 0;

}

int32_t conn_rbuff_flush(struct conn_t *conn) {

    // remove the request from the buffer
    size_t remain = conn->rbuff_size - conn->rbuff_read;
    size_t newsize;
    if (remain) {
        newsize = CEIL(remain + 1, 4096);
    } else {
        newsize = 4096;
    }
    uint8_t *newbuff = PyMem_RawCalloc(newsize + 1, sizeof(uint8_t));
    if (!newbuff) {
        log_error("conn_rbuff_flush(): failed to flush rbuff");
        return -1;
    }
    if (remain) {
        memcpy(newbuff, conn->rbuff + conn->rbuff_read, remain);
    }
    PyMem_RawFree(conn->rbuff);
    conn->rbuff = newbuff;
    conn->rbuff_size = remain;
    conn->rbuff_read = 0;
    conn->rbuff_max = newsize;

    conn->rbuff_size = remain;

    #if _FOO_KV_DEBUG == 1
    log_debug("conn_rbuff_flush(): successfully flushed rbuff");
    #endif

    return 0;

}

int32_t conn_write_response(struct conn_t *conn, const struct response_t *response) {
    // we should never get here if in state STATE_RES_WAITING
    // therefore wbuff should always be flushed

    // payload
    int32_t payloadlen = (response->payload) ? PyBytes_GET_SIZE(response->payload) : 0;
    // 2 for status + rest for data
    uint32_t msglen = sizeof(uint16_t) + payloadlen;
    // additional 2 for initial len
    uint32_t required_wbuff_size = sizeof(uint16_t) + msglen;

    // resize if necessary
    if (required_wbuff_size > conn->wbuff_max) {
        uint32_t newsize = CEIL(required_wbuff_size + 1, 1024);
        if (newsize > MAX_MSG_SIZE) {
            log_error("conn_write_response(): got response larger than max allowed size");
            return -1;
        }
        if (conn_wbuff_resize(conn, newsize) < 0) {
            return -1;
        }
    }

    #if _FOO_KV_DEBUG == 1
    char debug_buff[256];
    sprintf(debug_buff, "conn_write_response(): got response with status: %hd", response->status);
    log_debug(debug_buff);
    if (response->payload) {
        sprintf(debug_buff, "conn_write_response(): got response with data: %s", PyBytes_AS_STRING(response->payload));
        log_debug(debug_buff);
    }
    #endif

    uint16_t wmsglen = msglen;

    // write the response len
    memcpy(conn->wbuff, &wmsglen, sizeof(uint16_t));
    // write the response status
    memcpy(conn->wbuff + sizeof(uint16_t), &response->status, sizeof(int16_t));
    // write the data
    if (response->payload) {
        memcpy(conn->wbuff + sizeof(uint16_t) * 2, PyBytes_AS_STRING(response->payload), payloadlen);
        Py_DECREF(response->payload);
    }
    // update `wbuff_size`
    conn->wbuff_size = required_wbuff_size;

    return 0;

}

int32_t connarray_init(struct connarray_t *conns, int maxsize) {

    conns->size = 0;
    conns->maxsize = maxsize;
    conns->arr = PyMem_RawCalloc(maxsize, sizeof(struct conn_t *));
    if (conns->arr == NULL) {
        return -1;
    }

    return 0;

}

int32_t connarray_dealloc(struct connarray_t *conns) {
    
    for (int32_t ix = 0; ix < conns->maxsize; ix++) {
        struct conn_t *conn = conns->arr[ix];
        if (conn == NULL) {
            continue;
        }
        connarray_remove(conns, conn);
    }

    PyMem_RawFree(conns->arr);
    PyMem_RawFree(conns);

    return 0;

}

int32_t connarray_put(struct connarray_t *fd_to_conn, struct conn_t *conn) {

    if (fd_to_conn->maxsize <= conn->fd) {
        int new_maxsize = CEIL(conn->fd + 1, 8);
        struct conn_t **newarr = PyMem_RawCalloc(new_maxsize, sizeof(struct conn_t *));
        if (!newarr) {
            log_error("connarray_put(): failed to PyMem_RawCalloc memory!");
            return -1;
        }
        for (int32_t ix = 0; ix < fd_to_conn->maxsize; ix++) {
            if (fd_to_conn->arr[ix]) {
                newarr[ix] = fd_to_conn->arr[ix];
            }
        }
        PyMem_RawFree(fd_to_conn->arr);
        fd_to_conn->arr = newarr;
        fd_to_conn->maxsize = new_maxsize;
    }

    fd_to_conn->arr[conn->fd] = conn;
    fd_to_conn->size++;

    return 0;

}

int32_t connarray_remove(struct connarray_t *fd_to_conn, struct conn_t *conn) {

    if (sem_trywait(conn->lock) < 0) {
        log_warning("poll_loop(): conn lock is locked for ended connection");
        return -1;
    }
    char info_buff[64];
    sprintf(info_buff, "connarray_remove(): removing connection with fd: %d", conn->fd);
    log_info(info_buff);
    
    fd_to_conn->arr[conn->fd] = NULL;
    close(conn->fd);
    PyMem_RawFree(conn->rbuff);
    PyMem_RawFree(conn->wbuff);
    sem_post(conn->lock);
    sem_destroy(conn->lock);
    PyMem_RawFree(conn);

    fd_to_conn->size--;

    return 0;

}

int32_t accept_new_conn(struct connarray_t *fd_to_conn, int fd) {

    log_info("accept_new_conn(): got new connection");

    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int connfd;
    Py_BEGIN_ALLOW_THREADS
    connfd = accept(fd, (struct sockaddr *)&client_addr, &socklen);
    Py_END_ALLOW_THREADS
    if (connfd < 0) {
        log_error("accept_new_conn() error");
        return -1;
    }

    if (connfd < fd_to_conn->maxsize && fd_to_conn->arr[connfd] != NULL) {
        connarray_remove(fd_to_conn, fd_to_conn->arr[connfd]);
    }

    // set connfd to nonblocking mode
    fd_set_nb(connfd);

    struct conn_t *conn = conn_new(connfd);
    if (!conn) {
        log_error("accept_new_conn(): failed to allocate new connection!");
        return -1;
    }
    if (connarray_put(fd_to_conn, conn) < 0) {
        log_error("accept_new_conn(): connarray_put() error");
        close(connfd);
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    if (fd_to_conn->arr[connfd] != conn) {
        log_error("accept_new_conn(): got to end, but connection is not set...");
        return -1;
    }
    char debug_buff[256];
    sprintf(debug_buff, "accept_new_conn(): finished constructing new connection: %d", connfd);
    log_debug(debug_buff);
    #endif

    return 0;

}
