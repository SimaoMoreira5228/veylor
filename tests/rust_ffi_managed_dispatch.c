#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

_Static_assert(sizeof(vyl_managed_static_token) == 32, "token layout must match Rust");
_Static_assert(sizeof(vyl_managed_static_event) == 80, "event layout must match Rust");
_Static_assert(sizeof(vyl_managed_static_poll_report) == 32, "report layout must match Rust");
_Static_assert(VYL_HTTP1_STALE_TOKEN == 12, "stale code must match Rust");
_Static_assert(VYL_HTTP1_DUPLICATE == 13, "duplicate code must match Rust");
_Static_assert(VYL_HTTP1_GONE == 14, "gone code must match Rust");

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

static size_t read_to_end(int fd, char* out, size_t capacity) {
    size_t total = 0;
    ssize_t received = 0;
    for (;;) {
        received = recv(fd, out + total, capacity - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < capacity - 1);
    }
    out[total] = '\0';
    return total;
}

int main(void) {
    static const uint8_t get_method[] = "GET";
    static const uint8_t m_path[] = "/m";
    static const uint8_t c_path[] = "/c";
    static const uint8_t reason[] = "OK";
    static const uint8_t reply[] = "managed-hi";
    static const char request[] = "GET /m HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    static const char cancel_request[] = "GET /c HTTP/1.1\r\nHost: test\r\n\r\n";
    vyl_managed_static_config config = {0};
    vyl_managed_static_server* server = NULL;
    vyl_managed_static_close_counts counts = {0};
    vyl_managed_static_event events[8] = {0};
    uint8_t methods[256] = {0};
    uint8_t paths[4096] = {0};
    vyl_managed_static_poll_report report = {0};
    vyl_managed_static_token token = {0};
    uint16_t port = 0;
    int fd = -1;
    char response[4096] = {0};

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

    assert(vyl_managed_static_new(&config, &server) == VYL_HTTP1_OK);
    assert(server != NULL);
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){.data = get_method, .length = sizeof(get_method) - 1},
            (vyl_http1_bytes){.data = m_path, .length = sizeof(m_path) - 1},
            (uint64_t)-1
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){.data = get_method, .length = sizeof(get_method) - 1},
            (vyl_http1_bytes){.data = c_path, .length = sizeof(c_path) - 1},
            (uint64_t)-1
        ) == VYL_HTTP1_OK
    );
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);

    fd = connect_loopback(port);
    assert(send(fd, request, sizeof(request) - 1, 0) == (ssize_t)(sizeof(request) - 1));
    assert(
        vyl_managed_static_poll_events(
            server, 5000000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths), &report
        ) == VYL_HTTP1_OK
    );
    assert(report.copied == 1);
    assert(events[0].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_REQUEST);
    assert(events[0].method_len == sizeof(get_method) - 1);
    assert(memcmp(methods + events[0].method_offset, get_method, events[0].method_len) == 0);
    assert(events[0].path_len == sizeof(m_path) - 1);
    assert(memcmp(paths + events[0].path_offset, m_path, events[0].path_len) == 0);
    token = events[0].token;
    assert(
        vyl_managed_static_host_respond(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            (vyl_http1_bytes){.data = reply, .length = sizeof(reply) - 1}
        ) == VYL_HTTP1_OK
    );
    read_to_end(fd, response, sizeof(response));
    assert(close(fd) == 0);
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "managed-hi") != NULL);
    assert(
        vyl_managed_static_host_respond(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            (vyl_http1_bytes){.data = reply, .length = sizeof(reply) - 1}
        ) == VYL_HTTP1_DUPLICATE
    );

    fd = connect_loopback(port);
    assert(
        send(fd, cancel_request, sizeof(cancel_request) - 1, 0) == (ssize_t)(sizeof(cancel_request) - 1)
    );
    assert(
        vyl_managed_static_poll_events(
            server, 5000000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths), &report
        ) == VYL_HTTP1_OK
    );
    assert(report.copied == 1);
    token = events[0].token;
    assert(vyl_managed_static_host_cancel(server, token) == VYL_HTTP1_OK);
    assert(read_to_end(fd, response, sizeof(response)) == 0);
    assert(close(fd) == 0);
    assert(vyl_managed_static_host_cancel(server, token) == VYL_HTTP1_STALE_TOKEN);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == 1);
    assert(
        counts.completed + counts.peer_closed + counts.reset + counts.timeout +
            counts.protocol_error + counts.response_overflow + counts.transport_error +
            counts.shutdown == 1
    );
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    return 0;
}
