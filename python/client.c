#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

#include "util.h"

static int32_t send_req(int fd, const char *text) {

    uint32_t len = (uint32_t)strlen(text);
    if (len > MAX_MSG_SIZE) {
        return -1;
    }

    char wbuff[4 + MAX_MSG_SIZE];
    memcpy(wbuff, &len, 4);
    memcpy(wbuff + 4, text, len);
    int32_t err = write_all(fd, wbuff, 4 + len);
    if (err) {
        return err;
    }

    return 0;

}

static int32_t read_res(int fd) {

    // 4 bytes header
    char rbuff[4 + MAX_MSG_SIZE + 1];
    errno = 0;
    int32_t err = read_full(fd, rbuff, 4);
    if (err) {
        if (errno == 0) {
            log_error("EOF");
        } else {
            log_error("read() error");
        }
        return err;
    }

    uint32_t len;
    memcpy(&len, rbuff, 4);
    if (len > MAX_MSG_SIZE) {
        log_error("too long");
        return -1;
    }

    // reply body
    err = read_full(fd, rbuff + 4, len);
    if (err) {
        log_error("read() error");
        return -1;
    }

    // do stuff
    rbuff[4 + len] = '\0';
    printf("server says: %s\n", rbuff + 4);

    return 0;

}

static int32_t query(int fd, const char *text) {

    int32_t err = send_req(fd, text);
    if (err) {
        return err;
    }

    err = read_res(fd);
    return err;

}


int main() {

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        die("socket()");
    }

    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(8513);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK);
    int rv = connect(fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (rv) {
        die("connect()");
    }

    int err = 0;
    int num_inputs = 128;

    char *inputs[num_inputs];

    for (size_t ix = 0; ix < num_inputs; ix++) {
        
        int input_size = randint(16, 60000);
        char *input = calloc(input_size + 1, sizeof(char));
        for (size_t jx = 0; jx < input_size; jx++) {
            input[jx] = randint('a', 'z');
        }
        input[input_size] = '\0';

        inputs[ix] = input;

    }

    for (size_t ix = 0; ix < num_inputs; ix++) {
        err = send_req(fd, inputs[ix]);
        if (err) {
            goto L_DONE;
        }
    }

    log_error("finished sending requests");

    for (size_t ix = 0; ix < num_inputs; ix++) {
        err = read_res(fd);
        if (err) {
            goto L_DONE;
        }
    }

L_DONE:
    close(fd);
    return 0;

}
