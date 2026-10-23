#define _POSIX_C_SOURCE 200809L

#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

_Static_assert(sizeof(vyl_managed_static_token) == 32, "token layout must match Rust");
_Static_assert(sizeof(vyl_managed_static_event) == 80, "event layout must match Rust");
_Static_assert(sizeof(vyl_managed_static_poll_report) == 32, "report layout must match Rust");
_Static_assert(sizeof(vyl_managed_static_trailer_report) == 40, "trailer report layout must match Rust");
_Static_assert(VYL_HTTP1_WOULD_BLOCK == 7, "would-block code must match Rust");
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

static void wait_stream_writable(vyl_managed_static_server* server, vyl_managed_static_token token) {
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 10000000};
    for (int i = 0; i < 500; i++) {
        uint32_t writable = 0;
        assert(vyl_managed_static_host_is_writable(server, token, &writable) == VYL_HTTP1_OK);
        if (writable == 1) {
            return;
        }
        nanosleep(&pause, NULL);
    }
    assert(0 && "stream never armed writable");
}

int main(void) {
    static const uint8_t get_method[] = "GET";
    static const uint8_t post_method[] = "POST";
    static const uint8_t m_path[] = "/m";
    static const uint8_t c_path[] = "/c";
    static const uint8_t s_path[] = "/s";
    static const uint8_t t_path[] = "/t";
    static const uint8_t tag_name[] = "x-tag";
    static const char chunked_request[] = "POST /t HTTP/1.1\r\nHost: test\r\n"
                                          "Transfer-Encoding: chunked\r\n"
                                          "Connection: close\r\n\r\n"
                                          "5\r\nhello\r\n0\r\nX-Tag: v1\r\n\r\n";
    static const uint8_t reason[] = "OK";
    static const uint8_t reply[] = "managed-hi";
    static const uint8_t first_chunk[] = "hi";
    static const uint8_t second_chunk[] = "yo";
    static const char request[] = "GET /m HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    static const char stream_request[] = "GET /s HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
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
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){.data = get_method, .length = sizeof(get_method) - 1},
            (vyl_http1_bytes){.data = s_path, .length = sizeof(s_path) - 1},
            (uint64_t)-1
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_managed_static_register_managed_route_with_trailers(
            server,
            (vyl_http1_bytes){.data = post_method, .length = sizeof(post_method) - 1},
            (vyl_http1_bytes){.data = t_path, .length = sizeof(t_path) - 1},
            (uint64_t)-1,
            (const vyl_http1_bytes[]){{.data = tag_name, .length = sizeof(tag_name) - 1}},
            1
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

    fd = connect_loopback(port);
    assert(
        send(fd, chunked_request, sizeof(chunked_request) - 1, 0) ==
        (ssize_t)(sizeof(chunked_request) - 1)
    );
    assert(
        vyl_managed_static_poll_events(
            server, 5000000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths), &report
        ) == VYL_HTTP1_OK
    );
    assert(report.copied == 1);
    token = events[0].token;
    uint8_t staged[16] = {0};
    size_t staged_total = 0;
    uint32_t body_end = 0;
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    for (int i = 0; i < 5000 && body_end == 0; i++) {
        uint8_t chunk[16] = {0};
        size_t pulled = 0;
        uint32_t end = 0;
        assert(
            vyl_managed_static_host_pull_body(server, token, chunk, sizeof(chunk), &pulled, &end) ==
            VYL_HTTP1_OK
        );
        assert(staged_total + pulled <= sizeof(staged));
        memcpy(staged + staged_total, chunk, pulled);
        staged_total += pulled;
        body_end = end;
        if (body_end == 0) {
            nanosleep(&pause, NULL);
        }
    }
    assert(body_end == 1);
    assert(staged_total == 5);
    assert(memcmp(staged, "hello", 5) == 0);
    uint8_t short_names[2] = {0};
    uint8_t short_values[2] = {0};
    vyl_managed_static_trailer_report trailer_report = {0};
    assert(
        vyl_managed_static_host_pull_trailers(
            server, token, short_names, sizeof(short_names), short_values, sizeof(short_values),
            &trailer_report
        ) == VYL_HTTP1_OK
    );
    assert(trailer_report.fields == 1);
    assert(trailer_report.names_copied == 0 && trailer_report.values_copied == 0);
    assert(trailer_report.names_required == 5 && trailer_report.values_required == 2);
    uint8_t names[5] = {0};
    uint8_t values[2] = {0};
    assert(
        vyl_managed_static_host_pull_trailers(
            server, token, names, sizeof(names), values, sizeof(values), &trailer_report
        ) == VYL_HTTP1_OK
    );
    assert(trailer_report.fields == 1);
    assert(trailer_report.names_copied == 5 && trailer_report.values_copied == 2);
    assert(trailer_report.names_required == 0 && trailer_report.values_required == 0);
    assert(memcmp(names, "X-Tag", 5) == 0);
    assert(memcmp(values, "v1", 2) == 0);
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

    fd = connect_loopback(port);
    assert(
        send(fd, stream_request, sizeof(stream_request) - 1, 0) ==
        (ssize_t)(sizeof(stream_request) - 1)
    );
    assert(
        vyl_managed_static_poll_events(
            server, 5000000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths), &report
        ) == VYL_HTTP1_OK
    );
    assert(report.copied == 1);
    token = events[0].token;
    assert(
        vyl_managed_static_host_write_stream(
            server,
            token,
            (vyl_http1_bytes){.data = first_chunk, .length = sizeof(first_chunk) - 1}
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(
        vyl_managed_static_host_begin_stream(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1}
        ) == VYL_HTTP1_OK
    );
    wait_stream_writable(server, token);
    assert(
        vyl_managed_static_host_write_stream(
            server,
            token,
            (vyl_http1_bytes){.data = first_chunk, .length = sizeof(first_chunk) - 1}
        ) == VYL_HTTP1_OK
    );
    wait_stream_writable(server, token);
    assert(
        vyl_managed_static_host_write_stream(
            server,
            token,
            (vyl_http1_bytes){.data = second_chunk, .length = sizeof(second_chunk) - 1}
        ) == VYL_HTTP1_OK
    );
    wait_stream_writable(server, token);
    assert(vyl_managed_static_host_end_stream(server, token) == VYL_HTTP1_OK);
    read_to_end(fd, response, sizeof(response));
    assert(close(fd) == 0);
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "Transfer-Encoding: chunked") != NULL);
    assert(strstr(response, "2\r\nhi\r\n") != NULL);
    assert(strstr(response, "2\r\nyo\r\n") != NULL);
    assert(strstr(response, "0\r\n\r\n") != NULL);
    assert(vyl_managed_static_host_end_stream(server, token) == VYL_HTTP1_GONE);

    static const uint8_t checksum_name[] = "x-checksum";
    static const uint8_t checksum_value[] = "abc";
    static const uint8_t tag_value[] = "v1";
    fd = connect_loopback(port);
    assert(
        send(fd, stream_request, sizeof(stream_request) - 1, 0) ==
        (ssize_t)(sizeof(stream_request) - 1)
    );
    assert(
        vyl_managed_static_poll_events(
            server, 5000000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths), &report
        ) == VYL_HTTP1_OK
    );
    assert(report.copied == 1);
    token = events[0].token;
    assert(
        vyl_managed_static_host_begin_stream(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1}
        ) == VYL_HTTP1_OK
    );
    wait_stream_writable(server, token);
    assert(
        vyl_managed_static_host_write_stream(
            server,
            token,
            (vyl_http1_bytes){.data = first_chunk, .length = sizeof(first_chunk) - 1}
        ) == VYL_HTTP1_OK
    );
    wait_stream_writable(server, token);
    static const vyl_http1_field resp_trailers[] = {
        {
            .name = {.data = checksum_name, .length = sizeof(checksum_name) - 1},
            .value = {.data = checksum_value, .length = sizeof(checksum_value) - 1},
        },
        {
            .name = {.data = tag_name, .length = sizeof(tag_name) - 1},
            .value = {.data = tag_value, .length = sizeof(tag_value) - 1},
        },
    };
    assert(
        vyl_managed_static_host_end_stream_with_trailers(
            server, token, resp_trailers, sizeof(resp_trailers) / sizeof(resp_trailers[0])
        ) == VYL_HTTP1_OK
    );
    read_to_end(fd, response, sizeof(response));
    assert(close(fd) == 0);
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "Transfer-Encoding: chunked") != NULL);
    assert(strstr(response, "2\r\nhi\r\n") != NULL);
    assert(strstr(response, "0\r\nx-checksum: abc\r\nx-tag: v1\r\n\r\n") != NULL);
    assert(
        vyl_managed_static_host_end_stream_with_trailers(
            server, token, resp_trailers, sizeof(resp_trailers) / sizeof(resp_trailers[0])
        ) == VYL_HTTP1_GONE
    );

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == 4);
    assert(
        counts.completed + counts.peer_closed + counts.reset + counts.timeout +
            counts.protocol_error + counts.response_overflow + counts.transport_error +
            counts.shutdown == 4
    );
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    return 0;
}
