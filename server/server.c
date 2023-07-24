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

#include "dispatch.h"
#include "server.h"
#include "util.h"


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

int32_t connarray_init(struct ConnArray *conns, int maxsize) {

    conns->size = 0;
    conns->maxsize = maxsize;
    conns->arr = calloc(maxsize, sizeof(struct Conn *));
    if (conns->arr == NULL) {
        return -1;
    }

    return 0;

}

int32_t connarray_dealloc(struct ConnArray *conns) {
    
    for (int32_t ix = 0; ix < conns->maxsize; ix++) {
        struct Conn *conn = conns->arr[ix];
        if (conn == NULL) {
            continue;
        }
        connarray_remove(conns, conn);
    }

    free(conns->arr);
    free(conns);

    return 0;

}

int32_t connarray_put(struct ConnArray *fd_to_conn, struct Conn *conn) {

    if (fd_to_conn->maxsize <= conn->fd) {
        int new_maxsize = CEIL(conn->fd + 1, 8);
        struct Conn **newarr = calloc(new_maxsize, sizeof(struct Conn *));
        if (!newarr) {
            log_error("connarray_put(): failed to calloc memory!");
            return -1;
        }
        for (int32_t ix = 0; ix < fd_to_conn->maxsize; ix++) {
            if (fd_to_conn->arr[ix]) {
                newarr[ix] = fd_to_conn->arr[ix];
            }
        }
        free(fd_to_conn->arr);
        fd_to_conn->arr = newarr;
        fd_to_conn->maxsize = new_maxsize;
    }

    fd_to_conn->arr[conn->fd] = conn;
    fd_to_conn->size++;

    return 0;

}

int32_t connarray_remove(struct ConnArray *fd_to_conn, struct Conn *conn) {

    if (sem_trywait(conn->lock) < 0) {
        log_warning("poll_loop(): conn lock is locked for ended connection");
        return -1;
    }
    char info_buff[64];
    sprintf(info_buff, "connarray_remove(): removing connection with fd: %d", conn->fd);
    log_info(info_buff);
    
    fd_to_conn->arr[conn->fd] = NULL;
    close(conn->fd);
    free(conn->rbuff);
    free(conn->wbuff);
    sem_post(conn->lock);
    sem_destroy(conn->lock);
    free(conn);

    fd_to_conn->size--;

    return 0;

}

int32_t accept_new_conn(struct ConnArray *fd_to_conn, int fd) {

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

    // set connfd to nonblocking mode
    fd_set_nb(connfd);

    struct Conn *conn = conn_new(connfd);
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

int32_t connection_io(foo_kv_server *server, struct Conn *conn) {

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: starting for conn with fd", conn->fd);
    log_debug(debug_buffer);
    #endif

    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: about to acquire conn lock", conn->fd);
    log_debug(debug_buffer);
    #endif
    if (sem_trywait(conn->lock)) {
        if (errno == EINVAL) {
            log_error("connection_io(): sem_trywait() failed");
            conn->state = STATE_END;
            return -1;
        }
        #if _FOO_KV_DEBUG == 1
        sprintf(debug_buffer, "connection_io(): conn_fd: %d: another thread is already handling request, exiting", conn->fd);
        log_debug(debug_buffer);
        #endif
        return -1;
    }
    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: successfully acquired conn lock", conn->fd);
    log_debug(debug_buffer);
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: about to acquire io_conns_lock", conn->fd);
    log_debug(debug_buffer);
    #endif
    Py_BEGIN_ALLOW_THREADS
    if (sem_wait(server->io_conns_sem)) {
        log_error("connection_io(): sem_wait() failed");
        conn->state = STATE_END;
        return -1;
    }
    Py_END_ALLOW_THREADS
    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: successfully acquired io_conns_lock", conn->fd);
    log_debug(debug_buffer);
    #endif
    int32_t err;

    while (1) {
        switch (conn->state) {
            case STATE_REQ:
                err = state_req(conn);
                if (err) {
                    goto CONNECTION_IO_END;
                }
                continue;
            case STATE_RES:
                err = state_res(conn);
                if (err) {
                    goto CONNECTION_IO_END;
                }
                continue;
            case STATE_DISPATCH:
                err = state_dispatch(server, conn);
                if (err) {
                    goto CONNECTION_IO_END;
                }
                continue;
            case STATE_REQ_WAITING:
                #if _FOO_KV_DEBUG == 1
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: entered STATE_REQ_WAITING", conn->fd);
                log_debug(debug_buffer);
                #endif
                err = 0;
                goto CONNECTION_IO_END;
            case STATE_RES_WAITING:
                #if _FOO_KV_DEBUG == 1
                log_debug("connection_io(): entered STATE_RES_WAITING");
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: entered STATE_RES_WAITING", conn->fd);
                log_debug(debug_buffer);
                #endif
                err = 0;
                goto CONNECTION_IO_END;
            case STATE_END:
                #if _FOO_KV_DEBUG == 1
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: entered STATE_END", conn->fd);
                log_debug(debug_buffer);
                #endif
                err = -1;
                goto CONNECTION_IO_END;
            default:
                #if _FOO_KV_DEBUG == 1
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: Got invalid state", conn->fd);
                log_debug(debug_buffer);
                #else
                log_error("connection_io(): Got invalid state");
                #endif
                conn->state = STATE_END;
                err = -1;
                goto CONNECTION_IO_END;
        }
    }

CONNECTION_IO_END:

    if (sem_post(conn->lock)) {
        log_error("connection_io(): sem_post() failed");
        conn->state = STATE_END;
        return -1;
    }
    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: released conn lock", conn->fd);
    log_debug(debug_buffer);
    #endif
    if (sem_post(server->io_conns_sem)) {
        log_error("connection_io(): sem_post() failed");
        conn->state = STATE_END;
        return -1;
    }
    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: released io_conns_lock", conn->fd);
    log_debug(debug_buffer);
    #endif

    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: finished for conn with fd", conn->fd);
    log_debug(debug_buffer);
    #endif

    return err;

}

int32_t state_req(struct Conn *conn) {

    #if _FOO_KV_DEBUG == 1
    log_debug("state_req(): beginning");
    #endif
    
    int32_t res;

    do {
        while ((res = try_fill_buffer(conn)) > 0) {}
        if (res < 0) {
            return res;
        }
        res = try_one_request(conn);
        if (res < 0) {
            return res;
        }
    } while (conn->state == STATE_REQ);

    return 0;

}


int32_t try_fill_buffer(struct Conn *conn) {
    // returns a negative number on error
    // returns 0 to request stop iterating
    // returns 1 to request continue iterating

    if (conn->rbuff_size > conn->rbuff_max) {
        log_error("try_fill_buffer(): assertion 1: conn->rbuff_size is out of sync with conn->rbuff_max");
        conn->state = STATE_END;
        return -1;
    }

    ssize_t rv = 0;
    do {
        size_t cap = conn->rbuff_max - conn->rbuff_size;
        if (!cap) {
            // this could mean the client is sending more data than we can fit in our buffer
            // return 0 so try_one_request can deal with it
            log_warning("try_fill_buffer(): hit cap: will try resize");
            return 0;
        }
        Py_BEGIN_ALLOW_THREADS
        rv = read(conn->fd, conn->rbuff + conn->rbuff_size, cap);
        Py_END_ALLOW_THREADS
    } while (rv < 0 && errno == EINTR);

    if (rv < 0) {
        if (errno != EAGAIN) {
            log_error("try_fill_buffer(): read() error: unknown");
            conn->state = STATE_END;
            return -1;
        }
        // DO NOT fiddle with this
        // just loop if you get eagain
        // this will cause the caller to go down 1 of 2 branches:
        //  1. try_one_request finds enough data to process a request, and will set the state to STATE_RES
        //  2. try_one_request will not find enough data, and we will move along to another connection
        #if _FOO_KV_DEBUG == 1
        log_debug("try_fill_buffer(): no incoming data");
        #endif
        conn->state = STATE_REQ_WAITING;
        return 0;
    }

    if (rv == 0) {
        if (conn->rbuff_size > 0) {
            log_error("try_fill_buffer(): unexpected EOF");
        }
        conn->state = STATE_END;
        return -1;
    }

    conn->rbuff_size += (size_t)rv;

    if (conn->rbuff_size > conn->rbuff_max) {
        log_error("try_fill_buffer(): assertion 2: conn->rbuff_size is out of sync with conn->rbuff_max");
        conn->state = STATE_END;
        return -1;
    }

    return 1;

}


int32_t try_one_request(struct Conn *conn) {

    // check if we have 4 bytes in rbuff
    // if not, we will try again next iteration
    if (conn->rbuff_size - conn->rbuff_read < 4) {
        if (conn->rbuff_max - conn->rbuff_read < 1024) {
            if (conn_rbuff_flush(conn) < 0) {
                conn->state = STATE_END;
                return -1;
            }
        }
        return 0;
    }

    uint32_t len;
    memcpy(&len, conn->rbuff + conn->rbuff_read, 4);
    uint32_t msglen = len + 4;

    if (conn->rbuff_max - conn->rbuff_read < msglen) {
        if (msglen > MAX_MSG_SIZE) {
            log_error("try_one_request(): got msg that exceeds MAX_MSG_SIZE");
            conn->state = STATE_END;
            return -1;
        }
        #if _FOO_KV_DEBUG == 1
        log_debug("try_one_request(): need to resize rbuff");
        #endif
        if (conn_rbuff_resize(conn, CEIL(msglen + 1, 4096)) < 0) {
            log_error("try_one_request(): conn_rbuff_resize() failed");
            conn->state = STATE_END;
            return -1;
        }
        #if _FOO_KV_DEBUG == 1
        log_debug("success: resized rbuff");
        #endif
    }

    if (conn->rbuff_size - conn->rbuff_read < msglen) {
        // not enough data, will try again next iteration.
        return 0;
    }

    conn->state = STATE_DISPATCH;

    return 0;

}

int32_t state_dispatch(foo_kv_server *server, struct Conn *conn) {

    #if _FOO_KV_DEBUG == 1
    log_debug("state_dispatch(): beginning");
    #endif

    uint32_t len;
    uint8_t *rbuff_start = conn->rbuff + conn->rbuff_read;
    memcpy(&len, rbuff_start, 4);
    rbuff_start += 4;

    struct Response *response = dispatch(server, conn->connid, rbuff_start, len);
    if (response == NULL) {
        log_error("try_one_request(): dispatch returned null");
        struct Response *err_response = calloc(1, sizeof(struct Response));
        err_response->status = RES_UNKNOWN;
        err_response->datalen = 0;

        conn_write_response(conn, err_response);
        free(err_response);
    } else {
        conn_write_response(conn, response);
        free(response);
    }

    conn->rbuff_read += len + 4;

    // reset rbuff
    // TODO figure out how to handle this
    //conn_flush(conn, flushsize);

    // change state
    conn->state = STATE_RES;

    return 0;

}

int32_t state_res(struct Conn *conn) {

    #if _FOO_KV_DEBUG == 1
    log_debug("state_res(): beginning");
    #endif

    while (try_flush_buffer(conn)) {}
    return 0;

}

int32_t try_flush_buffer(struct Conn *conn) {

    ssize_t rv = 0;
    do {
        size_t remain = conn->wbuff_size - conn->wbuff_sent;
        Py_BEGIN_ALLOW_THREADS
        rv = write(conn->fd, conn->wbuff + conn->wbuff_sent, remain);
        Py_END_ALLOW_THREADS
    } while (rv < 0 && errno == EINTR);

    if (rv < 0) {
        if (errno != EAGAIN) {
            log_error("try_flush_buffer(): write() error");
            conn->state = STATE_END;
        }
        // see snarky comment in try_one_request
        // same applies here
        conn->state = STATE_RES_WAITING;
        return 0;
    }

    conn->wbuff_sent += (size_t)rv;
    if (conn->wbuff_sent > conn->wbuff_size) {
        log_error("try_flush_buffer(): wbuff_sent > wbuff_size");
        return 0;
    }

    if (conn->wbuff_sent == conn->wbuff_size) {
        #if _FOO_KV_DEBUG == 1
        log_debug("try_flush_buffer(): successfully sent response");
        #endif
        // response was fully sent
        // success case
        conn->state = STATE_REQ;
        conn->wbuff_sent = 0;
        conn->wbuff_size = 0;
        return 0;
    }

    // still have data in wbuff
    // iterate again
    return 1;

}
