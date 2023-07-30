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

#include "util.h"
#include "connection.h"
#include "connection_io.h"
#include "dispatch.h"

// CHANGE ME
#define _FOO_KV_DEBUG 1

int32_t connection_io(foo_kv_server *server, struct conn_t *conn) {

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
        return 0;
    }
    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: successfully acquired conn lock", conn->fd);
    log_debug(debug_buffer);
    #endif
    int32_t err;

    while (1) {
        switch (conn->state) {
            case STATE_REQ:
                #if _FOO_KV_DEBUG == 1
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: entered STATE_REQ", conn->fd);
                log_debug(debug_buffer);
                #endif
                err = state_req(conn);
                if (err) {
                    goto CONNECTION_IO_END;
                }
                continue;
            case STATE_RES:
                #if _FOO_KV_DEBUG == 1
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: entered STATE_RES", conn->fd);
                log_debug(debug_buffer);
                #endif
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
            case STATE_TERM:
                #if _FOO_KV_DEBUG == 1
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: got STATE_TERM, shouldn't get here", conn->fd);
                log_error(debug_buffer);
                #else
                log_error("connection_io(): Got STATE_TERM, shouldn't get here");
                #endif
                err = 0;
                goto CONNECTION_IO_END;
            default:
                #if _FOO_KV_DEBUG == 1
                sprintf(debug_buffer, "connection_io(): conn_fd: %d: Got invalid state", conn->fd);
                log_error(debug_buffer);
                #else
                log_error("connection_io(): Got invalid state");
                #endif
                err = -1;
                goto CONNECTION_IO_END;
        }
    }

CONNECTION_IO_END:

    if (sem_post(conn->lock)) {
        log_error("connection_io(): sem_post() failed");
        err = -1;
    }
    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: released conn lock", conn->fd);
    log_debug(debug_buffer);
    #endif

    #if _FOO_KV_DEBUG == 1
    sprintf(debug_buffer, "connection_io(): conn_fd: %d: finished for conn: err: %d", conn->fd, err);
    log_debug(debug_buffer);
    #endif

    if (err) {
        conn->state = STATE_END;
    }

    return err;

}

int32_t state_req(struct conn_t *conn) {

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

        #if _FOO_KV_DEBUG == 1
        if (conn->state != STATE_REQ) {
            char debug_buffer[256];
            sprintf(debug_buffer, "state_req(): end of loop: state: %d: will continue if state is: %d", conn->state, STATE_REQ);
            log_debug(debug_buffer);
        }
        #endif

    } while (conn->state == STATE_REQ);

    return 0;

}


int32_t try_fill_buffer(struct conn_t *conn) {
    // returns a negative number on error
    // returns 0 to request stop iterating
    // returns 1 to request continue iterating

    #if _FOO_KV_DEBUG == 1
    if (conn->rbuff_size > conn->rbuff_max) {
        log_error("try_fill_buffer(): assertion 1: conn->rbuff_size is out of sync with conn->rbuff_max");
        conn->state = STATE_END;
        return -1;
    }
    #endif

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
        conn->err = errno;
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
        log_debug("try_fill_buffer(): got EAGAIN: continuing to try_one_request");
        #endif
        return 0;
    }

    conn->err = 0;

    if (rv == 0) {
        if (conn->rbuff_size > 0) {
            log_error("try_fill_buffer(): unexpected EOF");
        }
        conn->state = STATE_END;
        return -1;
    }

    #if _FOO_KV_DEBUG == 1
    char debug_buffer[256];
    if (conn->state != STATE_REQ) {
        sprintf(debug_buffer, "try_fill_buffer(): state is reverting from %d to %d", conn->state, STATE_REQ);
        log_debug(debug_buffer);
    }
    sprintf(debug_buffer, "try_fill_buffer(): bytes read: %ld", rv);
    log_debug(debug_buffer);
    #endif
    conn->state = STATE_REQ;
    conn->rbuff_size += (size_t)rv;

    if (conn->rbuff_size > conn->rbuff_max) {
        log_error("try_fill_buffer(): assertion 2: conn->rbuff_size is out of sync with conn->rbuff_max");
        conn->state = STATE_END;
        return -1;
    }

    return rv;

}


int32_t try_one_request(struct conn_t *conn) {

    // check if we have 4 bytes in rbuff
    // if not, we will try again next iteration
    if (conn->rbuff_size - conn->rbuff_read < sizeof(uint16_t)) {
        if (conn->rbuff_max - conn->rbuff_read < 1024) {
            if (conn_rbuff_flush(conn) < 0) {
                conn->state = STATE_END;
                return -1;
            }
        }
        if (conn->err) {
            conn->state = STATE_REQ_WAITING;
        }
        return 0;
    }

    uint16_t len;
    memcpy(&len, conn->rbuff + conn->rbuff_read, sizeof(uint16_t));
    uint32_t msglen = len + sizeof(uint16_t);

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
        #if _FOO_KV_DEBUG == 1
        char debug_buff[256];
        sprintf(debug_buff, "try_one_request(): got msglen: %hu, currently read: %ld, currently unread: %ld",
                msglen, conn->rbuff_read, conn->rbuff_size - conn->rbuff_read);
        log_debug(debug_buff);
        #endif
        if (conn->err) {
            // unless an err is set, in which case we break
            #if _FOO_KV_DEBUG == 1
            log_debug("try_one_request(): previous read resulted in EAGAIN: will exit loop");
            #endif
            conn->state = STATE_REQ_WAITING;
        }
        return 0;
    }

    conn->state = STATE_DISPATCH;

    return 0;

}

int32_t state_dispatch(foo_kv_server *server, struct conn_t *conn) {

    #if _FOO_KV_DEBUG == 1
    log_debug("state_dispatch(): beginning");
    #endif

    uint16_t len;
    int32_t err;
    uint8_t *rbuff_start = conn->rbuff + conn->rbuff_read;
    memcpy(&len, rbuff_start, sizeof(uint16_t));
    rbuff_start += sizeof(uint16_t);

    struct response_t *response = PyMem_RawCalloc(1, sizeof(struct response_t));
    if (!response) {
        log_error("state_dispatch(): calloc returned null");
        return -1;
    }

    err = dispatch(server, conn->connid, rbuff_start, len, response);
    conn_write_response(conn, response);
    PyMem_RawFree(response);

    // 4 for the len indicator + rest of message
    conn->rbuff_read += sizeof(uint16_t) + len;

    // change state
    conn->state = STATE_RES;

    return err;

}

int32_t state_res(struct conn_t *conn) {

    #if _FOO_KV_DEBUG == 1
    log_debug("state_res(): beginning");
    #endif

    while (try_flush_buffer(conn)) {}
    return 0;

}

int32_t try_flush_buffer(struct conn_t *conn) {

    #if _FOO_KV_DEBUG == 1
    if (conn->wbuff_size <= conn->wbuff_sent) {
        log_error("try_flush_buffer(): assertion 1 failed");
        return 0;
    }
    if (conn->wbuff_max < conn->wbuff_size) {
        log_error("try_flush_buffer(): assertion 2 failed");
        return 0;
    }
    #endif

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
