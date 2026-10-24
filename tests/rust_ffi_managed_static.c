#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

_Static_assert(
    sizeof(vyl_managed_static_close_counts) == 8 * 8,
    "close counts layout must match Rust"
);
_Static_assert(VYL_HTTP1_BUSY == 15, "busy code must match Rust VylResult::Busy");

static int connect_loopback(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    assert(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
    assert(connect(fd, (const struct sockaddr*)&addr, sizeof(addr)) == 0);
    return fd;
}

int main(void) {
    static const uint8_t method[] = "GET";
    static const uint8_t path[] = "/hello";
    static const uint8_t reason[] = "OK";
    static const uint8_t body[] = "hello";
    static const char request[] = "GET /hello HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    static const uint8_t post_method[] = "POST";
    static const uint8_t tiny_path[] = "/tiny";
    static const uint8_t tiny_body[] = "tiny";
    static const char over_request[] = "POST /tiny HTTP/1.1\r\nHost: test\r\nContent-Length: 5\r\nConnection: close\r\n\r\n";
    static const uint8_t mount_prefix[] = "/assets/";
    static const uint8_t mount_file_body[] = "mount-bytes";
    static const char mount_request[] = "GET /assets/hello.txt HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    static const char range_request[] = "GET /assets/hello.txt HTTP/1.1\r\nHost: test\r\nRange: bytes=0-4\r\nConnection: close\r\n\r\n";
    vyl_managed_static_config config = {0};
    vyl_managed_static_server* server = NULL;
    vyl_managed_static_close_counts counts = {0};
    uint16_t port = 0;
    int fd = -1;
    char response[1024] = {0};
    size_t total = 0;
    ssize_t received = 0;

    config.abi_version = VYL_MANAGED_STATIC_ABI_VERSION;
    config.struct_size = sizeof(config);
    config.request_head_timeout_ns = 5000000000u;
    config.body_progress_timeout_ns = 5000000000u;
    config.response_write_timeout_ns = 5000000000u;
    config.keep_alive_idle_timeout_ns = 30000000000u;
    config.shutdown_grace_ns = 5000000000u;
    config.drain_idle_linger_ns = 200000000u;
    config.max_accepts_per_turn = 8;
    config.max_events_per_turn = 64;
    config.byte_budget_per_turn = 65536;
    config.poll_wait_ms = 10;
    config.port = 0;

    char mount_dir[64] = {0};
    assert(snprintf(mount_dir, sizeof(mount_dir), "/tmp/veylor-mount-%d", (int)getpid()) > 0);
    if (mkdir(mount_dir, 0700) != 0) {
        assert(errno == EEXIST);
    }
    char mount_path[96] = {0};
    assert(snprintf(mount_path, sizeof(mount_path), "%s/hello.txt", mount_dir) > 0);
    FILE* mount_file = fopen(mount_path, "w");
    assert(mount_file != NULL);
    assert(fwrite(mount_file_body, 1, sizeof(mount_file_body) - 1, mount_file) == sizeof(mount_file_body) - 1);
    assert(fclose(mount_file) == 0);

    assert(vyl_managed_static_new(&config, &server) == VYL_HTTP1_OK);
    assert(server != NULL);
    assert(vyl_managed_static_set_worker_count(NULL, 2) == VYL_HTTP1_INVALID_ARGUMENT);
    assert(vyl_managed_static_set_worker_count(server, 0) == VYL_HTTP1_INVALID_ARGUMENT);
    assert(vyl_managed_static_set_worker_count(server, 17) == VYL_HTTP1_INVALID_ARGUMENT);
    assert(vyl_managed_static_set_worker_count(server, 2) == VYL_HTTP1_OK);
    assert(vyl_managed_static_enable_h2c(NULL, 0) == VYL_HTTP1_INVALID_ARGUMENT);
    assert(vyl_managed_static_enable_h2c(server, 0) == VYL_HTTP1_OK);
    assert(
        vyl_managed_static_register_static_route(
            server,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = path, .length = sizeof(path) - 1},
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1}
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_managed_static_register_static_route_with_body_cap(
            server,
            (vyl_http1_bytes){.data = post_method, .length = sizeof(post_method) - 1},
            (vyl_http1_bytes){.data = tiny_path, .length = sizeof(tiny_path) - 1},
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            (vyl_http1_bytes){.data = tiny_body, .length = sizeof(tiny_body) - 1},
            4
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_managed_static_register_file_mount(
            server,
            (vyl_http1_bytes){.data = mount_prefix, .length = sizeof(mount_prefix) - 1},
            (vyl_http1_bytes){.data = (const uint8_t*)mount_dir, .length = strlen(mount_dir)}
        ) == VYL_HTTP1_OK
    );
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_set_worker_count(server, 1) == VYL_HTTP1_BAD_STATE);
    assert(vyl_managed_static_enable_h2c(server, 0) == VYL_HTTP1_BAD_STATE);
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);

    fd = connect_loopback(port);
    assert(send(fd, request, sizeof(request) - 1, 0) == (ssize_t)(sizeof(request) - 1));
    for (;;) {
        received = recv(fd, response + total, sizeof(response) - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < sizeof(response) - 1);
    }
    assert(close(fd) == 0);
    response[total] = '\0';
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "hello") != NULL);

    fd = connect_loopback(port);
    assert(send(fd, over_request, sizeof(over_request) - 1, 0) == (ssize_t)(sizeof(over_request) - 1));
    total = 0;
    for (;;) {
        received = recv(fd, response + total, sizeof(response) - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < sizeof(response) - 1);
    }
    assert(close(fd) == 0);
    response[total] = '\0';
    assert(strstr(response, "HTTP/1.1 413") != NULL);
    assert(strstr(response, "Payload Too Large") != NULL);

    fd = connect_loopback(port);
    assert(send(fd, mount_request, sizeof(mount_request) - 1, 0) == (ssize_t)(sizeof(mount_request) - 1));
    total = 0;
    for (;;) {
        received = recv(fd, response + total, sizeof(response) - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < sizeof(response) - 1);
    }
    assert(close(fd) == 0);
    response[total] = '\0';
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "mount-bytes") != NULL);
    assert(strstr(response, "text/plain") != NULL);
    assert(strstr(response, "Last-Modified: ") != NULL);

    char last_modified[64] = {0};
    {
        const char* lm = strstr(response, "Last-Modified: ");
        assert(lm != NULL);
        lm += strlen("Last-Modified: ");
        const char* eol = strstr(lm, "\r\n");
        assert(eol != NULL && (size_t)(eol - lm) < sizeof(last_modified));
        memcpy(last_modified, lm, (size_t)(eol - lm));
    }

    fd = connect_loopback(port);
    assert(send(fd, range_request, sizeof(range_request) - 1, 0) == (ssize_t)(sizeof(range_request) - 1));
    total = 0;
    for (;;) {
        received = recv(fd, response + total, sizeof(response) - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < sizeof(response) - 1);
    }
    assert(close(fd) == 0);
    response[total] = '\0';
    assert(strstr(response, "HTTP/1.1 206") != NULL);
    assert(strstr(response, "Content-Range: bytes 0-4/11") != NULL);
    assert(strstr(response, "mount") != NULL);
    assert(strstr(response, "mount-bytes") == NULL);

    char cond_request[256] = {0};
    assert(snprintf(cond_request, sizeof(cond_request),
        "GET /assets/hello.txt HTTP/1.1\r\nHost: test\r\nIf-Modified-Since: %s\r\nConnection: close\r\n\r\n",
        last_modified) > 0);
    fd = connect_loopback(port);
    assert(send(fd, cond_request, strlen(cond_request), 0) == (ssize_t)strlen(cond_request));
    total = 0;
    for (;;) {
        received = recv(fd, response + total, sizeof(response) - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < sizeof(response) - 1);
    }
    assert(close(fd) == 0);
    response[total] = '\0';
    assert(strstr(response, "HTTP/1.1 304") != NULL);
    assert(strstr(response, "Last-Modified: ") != NULL);
    assert(strstr(response, "mount-bytes") == NULL);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == 5);
    assert(
        counts.completed + counts.peer_closed + counts.reset + counts.timeout +
            counts.protocol_error + counts.response_overflow + counts.transport_error +
            counts.shutdown == 5
    );
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    assert(remove(mount_path) == 0);
    assert(rmdir(mount_dir) == 0);
    return 0;
}
