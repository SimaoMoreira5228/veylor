#define _POSIX_C_SOURCE 200809L

#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
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

static void recv_exact(int fd, void* out, size_t length) {
    size_t total = 0;
    while (total < length) {
        ssize_t received = recv(fd, (char*)out + total, length - total, 0);
        assert(received > 0);
        total += (size_t)received;
    }
}

static size_t read_head(int fd, char* out, size_t capacity) {
    size_t total = 0;
    for (;;) {
        char byte = 0;
        assert(recv(fd, &byte, 1, 0) == 1);
        assert(total < capacity - 1);
        out[total++] = byte;
        if (total >= 4 && memcmp(out + total - 4, "\r\n\r\n", 4) == 0) {
            break;
        }
    }
    out[total] = '\0';
    return total;
}

 
static vyl_h2_field_to_encode h2_text_field(const char* name, const char* value) {
    vyl_h2_field_to_encode field = {0};
    field.name.data = (const uint8_t*)name;
    field.name.length = strlen(name);
    field.value.data = (const uint8_t*)value;
    field.value.length = strlen(value);
    field.representation = VYL_H2_WITHOUT_INDEXING;
    return field;
}

static void h2_ack_client(vyl_h2_client* client, size_t transferred) {
    size_t pending = 0;
    assert(vyl_h2_client_acknowledge(client, transferred, &pending) == VYL_HTTP1_OK);
    assert(pending == 0);
}

static void h2_send_all(int fd, const uint8_t* data, size_t length) {
    size_t total = 0;
    while (total < length) {
        ssize_t sent = send(fd, data + total, length - total, 0);
        assert(sent > 0);
        total += (size_t)sent;
    }
}

static size_t h2_read_frame(
    int fd,
    uint8_t* frame,
    size_t capacity,
    uint32_t* out_type,
    uint32_t* out_flags,
    uint32_t* out_stream
) {
    assert(capacity >= 9);
    recv_exact(fd, frame, 9);
    size_t length = ((size_t)frame[0] << 16) | ((size_t)frame[1] << 8) | (size_t)frame[2];
    assert(9 + length <= capacity);
    *out_type = frame[3];
    *out_flags = frame[4];
    *out_stream = (((uint32_t)frame[5] << 24) | ((uint32_t)frame[6] << 16) |
                   ((uint32_t)frame[7] << 8) | (uint32_t)frame[8]) &
                  0x7FFFFFFFu;
    if (length > 0) {
        recv_exact(fd, frame + 9, length);
    }
    return 9 + length;
}

static int h2_feed_client(
    vyl_h2_client* client,
    const uint8_t* frame,
    size_t frame_length,
    vyl_h2_action* actions,
    vyl_h2_field* fields,
    uint8_t* data,
    size_t* out_actions,
    size_t* out_fields
) {
    size_t consumed = 0;
    size_t actions_required = 0;
    size_t fields_required = 0;
    size_t data_required = 0;
    uint32_t connection_error = 0;
    int rc = vyl_h2_client_poll_feed(
        client, frame, frame_length, &consumed, actions, 16, fields, 64, data, 4096, out_actions,
        out_fields, &actions_required, &fields_required, &data_required, &connection_error
    );
    assert(consumed == frame_length);
    assert(rc == VYL_HTTP1_OK || rc == VYL_HTTP1_NEED_MORE);
    return rc;
}

int main(void) {
    static const uint8_t get_method[] = "GET";
    static const uint8_t post_method[] = "POST";
    static const uint8_t m_path[] = "/m";
    static const uint8_t c_path[] = "/c";
    static const uint8_t s_path[] = "/s";
    static const uint8_t t_path[] = "/t";
    static const uint8_t u_path[] = "/u";
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
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){.data = post_method, .length = sizeof(post_method) - 1},
            (vyl_http1_bytes){.data = u_path, .length = sizeof(u_path) - 1},
            (uint64_t)-1
        ) == VYL_HTTP1_OK
    );
    assert(vyl_managed_static_enable_h2c(server, 0) == VYL_HTTP1_OK);
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);
    uint16_t h2c_port = 0;
    assert(vyl_managed_static_h2c_addr(server, &h2c_port) == VYL_HTTP1_OK);
    assert(h2c_port != 0);
    assert(h2c_port != port);

    {
        vyl_managed_static_server* tmp = NULL;
        vyl_managed_static_close_counts tmp_counts = {0};
        uint16_t probe = 0xBEEF;
        assert(vyl_managed_static_h2c_addr(NULL, &probe) == VYL_HTTP1_INVALID_ARGUMENT);
        assert(vyl_managed_static_new(&config, &tmp) == VYL_HTTP1_OK);
        assert(tmp != NULL);
        assert(vyl_managed_static_h2c_addr(tmp, NULL) == VYL_HTTP1_INVALID_ARGUMENT);
        assert(vyl_managed_static_h2c_addr(tmp, &probe) == VYL_HTTP1_BAD_STATE);
        assert(probe == 0);
        assert(vyl_managed_static_enable_h2c(tmp, 0) == VYL_HTTP1_OK);
        assert(vyl_managed_static_h2c_addr(tmp, &probe) == VYL_HTTP1_BAD_STATE);
        assert(probe == 0);
        assert(vyl_managed_static_destroy(tmp) == VYL_HTTP1_OK);
        assert(vyl_managed_static_new(&config, &tmp) == VYL_HTTP1_OK);
        assert(vyl_managed_static_start(tmp) == VYL_HTTP1_OK);
        assert(vyl_managed_static_h2c_addr(tmp, &probe) == VYL_HTTP1_BAD_STATE);
        assert(probe == 0);
        assert(vyl_managed_static_stop_accepting(tmp) == VYL_HTTP1_OK);
        assert(vyl_managed_static_join(tmp, &tmp_counts) == VYL_HTTP1_OK);
        assert(vyl_managed_static_destroy(tmp) == VYL_HTTP1_OK);
    }

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

    static const uint8_t hello_body[] = "hello";
    fd = connect_loopback(port);
    assert(
        send(fd, chunked_request, sizeof(chunked_request) - 1, 0) ==
        (ssize_t)(sizeof(chunked_request) - 1)
    );
    int have_request = 0;
    int have_body = 0;
    size_t body_ready = 0;
    for (int i = 0; i < 500 && (!have_request || body_ready != sizeof(hello_body) - 1); i++) {
        assert(
            vyl_managed_static_poll_events(
                server, 100000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t k = 0; k < report.copied; k++) {
            if (events[k].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_REQUEST) {
                token = events[k].token;
                have_request = 1;
            }
            /* NOTE: body-available reuses method_len for staged-ready bytes and
               path_len for the end flag; see the Rust field-reuse docs. */
            if (events[k].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE) {
                assert(events[k].route_id == 0);
                assert(events[k].method_offset == 0 && events[k].path_offset == 0);
                assert(events[k].path_len == 0);
                body_ready = events[k].method_len;
                have_body = 1;
            }
        }
    }
    assert(have_request == 1);
    assert(have_body == 1);
    assert(body_ready == sizeof(hello_body) - 1);
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

    /* NOTE: this exchange pins the WRITABLE half on the wire. Each consumed write re-arms
       exactly one event, and each event admits exactly one write: the first chunk is
       consumed on the wire before the re-arm is observed, pinning release-then-signal. */
    static const uint8_t hi_chunk[] = "hi";
    static const uint8_t yo_chunk[] = "yo";
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
    assert(events[0].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_REQUEST);
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
            (vyl_http1_bytes){.data = hi_chunk, .length = sizeof(hi_chunk) - 1}
        ) == VYL_HTTP1_OK
    );
    {
        char head[1024] = {0};
        read_head(fd, head, sizeof(head));
        assert(strstr(head, "HTTP/1.1 200 OK") != NULL);
        assert(strstr(head, "Transfer-Encoding: chunked") != NULL);
        char frame[8] = {0};
        recv_exact(fd, frame, 3);
        assert(memcmp(frame, "2\r\n", 3) == 0);
        recv_exact(fd, frame, 2);
        assert(memcmp(frame, "hi", 2) == 0);
        recv_exact(fd, frame, 2);
        assert(memcmp(frame, "\r\n", 2) == 0);
    }
    int have_writable = 0;
    for (int i = 0; i < 500 && !have_writable; i++) {
        assert(
            vyl_managed_static_poll_events(
                server, 100000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t k = 0; k < report.copied; k++) {
            assert(events[k].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_WRITABLE);
            assert(memcmp(&events[k].token, &token, sizeof(token)) == 0);
            assert(events[k].route_id == 0);
            assert(events[k].method_len == 0 && events[k].path_len == 0);
            assert(events[k].method_offset == 0 && events[k].path_offset == 0);
            have_writable = 1;
        }
    }
    assert(have_writable == 1);
    /* NOTE: the observed event may predate the just-consumed write (coalesced staging dedupes
       repeats while one is pending), so the authoritative query gates the next admission. */
    wait_stream_writable(server, token);
    assert(
        vyl_managed_static_host_write_stream(
            server,
            token,
            (vyl_http1_bytes){.data = yo_chunk, .length = sizeof(yo_chunk) - 1}
        ) == VYL_HTTP1_OK
    );
    wait_stream_writable(server, token);
    assert(vyl_managed_static_host_end_stream(server, token) == VYL_HTTP1_OK);
    {
        char tail[16] = {0};
        recv_exact(fd, tail, sizeof("2\r\nyo\r\n0\r\n\r\n") - 1);
        assert(memcmp(tail, "2\r\nyo\r\n0\r\n\r\n", sizeof("2\r\nyo\r\n0\r\n\r\n") - 1) == 0);
    }
    assert(close(fd) == 0);
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

    /* NOTE: H2C managed-body flow through the h2c_addr port: handshake, HEADERS-open POST,
       DATA end, BODY_AVAILABLE with ready bytes plus the end flag, exact pull_body, then
       exact response bytes classified through the client handle. Grant frames interleave
       freely and feed through the handle; only HEADERS/DATA on stream 1 are asserted. */
    static const uint8_t h2_post_body[] = "hello";
    static const uint8_t h2_reply[] = "done";
    int h2fd = connect_loopback(h2c_port);
    const struct timeval h2_timeout = {.tv_sec = 5, .tv_usec = 0};
    assert(setsockopt(h2fd, SOL_SOCKET, SO_RCVTIMEO, &h2_timeout, sizeof(h2_timeout)) == 0);
    vyl_h2_client* h2 = NULL;
    assert(vyl_h2_client_new(&h2) == VYL_HTTP1_OK);
    assert(h2 != NULL);
    static uint8_t h2wire[16384] = {0};
    static uint8_t h2frame[16384] = {0};
    static vyl_h2_action h2actions[16] = {0};
    static vyl_h2_field h2fields[64] = {0};
    static uint8_t h2data[4096] = {0};
    size_t h2written = 0;
    size_t h2required = 0;
    size_t h2got = 0;
    size_t h2seen = 0;
    uint32_t h2type = 0;
    uint32_t h2flags = 0;
    uint32_t h2stream = 0;
    assert(
        vyl_h2_client_connection_preface(h2wire, sizeof(h2wire), &h2written, &h2required) ==
        VYL_HTTP1_OK
    );
    assert(h2written == 24);
    h2_send_all(h2fd, h2wire, h2written);
    assert(
        vyl_h2_client_send_settings(h2, NULL, 0, h2wire, sizeof(h2wire), &h2written, &h2required) ==
        VYL_HTTP1_OK
    );
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    int h2_server_settings = 0;
    for (int i = 0; i < 8 && !h2_server_settings; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_SETTINGS_ACK_REQUIRED) {
                assert(
                    vyl_h2_client_send_settings_ack(
                        h2, h2wire, sizeof(h2wire), &h2written, &h2required
                    ) == VYL_HTTP1_OK
                );
                h2_send_all(h2fd, h2wire, h2written);
                h2_ack_client(h2, h2written);
                h2_server_settings = 1;
            }
        }
    }
    assert(h2_server_settings == 1);
    int h2_settings_ack = 0;
    for (int i = 0; i < 8 && !h2_settings_ack; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        int rc = h2_feed_client(
            h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen
        );
        if (h2type == 4 && (h2flags & 1u) != 0) {
            assert(rc == VYL_HTTP1_NEED_MORE);
            h2_settings_ack = 1;
        }
    }
    assert(h2_settings_ack == 1);

    const vyl_h2_field_to_encode h2req[] = {
        h2_text_field(":method", "POST"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/u"),
        h2_text_field(":authority", "localhost"),
        h2_text_field("content-length", "5"),
    };
    uint32_t h2stream_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2req, sizeof(h2req) / sizeof(h2req[0]), 0, h2wire, sizeof(h2wire), &h2written,
            &h2stream_id, &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2stream_id == 1);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    assert(
        vyl_managed_static_poll_events(
            server, 5000000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths), &report
        ) == VYL_HTTP1_OK
    );
    assert(report.copied == 1);
    assert(events[0].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_REQUEST);
    assert(events[0].token.stream_id == 1);
    assert(events[0].method_len == sizeof(post_method) - 1);
    assert(memcmp(methods + events[0].method_offset, post_method, events[0].method_len) == 0);
    assert(events[0].path_len == sizeof(u_path) - 1);
    assert(memcmp(paths + events[0].path_offset, u_path, events[0].path_len) == 0);
    token = events[0].token;
    size_t h2consumed = 0;
    uint32_t h2end = 0;
    assert(
        vyl_h2_client_send_data(
            h2, 1,
            (vyl_http1_bytes){.data = h2_post_body, .length = sizeof(h2_post_body) - 1},
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2consumed, &h2written, &h2end,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2consumed == sizeof(h2_post_body) - 1);
    assert(h2end == (uint32_t)VYL_H2_END_STREAM);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    int h2_have_body = 0;
    for (int i = 0; i < 500 && !h2_have_body; i++) {
        assert(
            vyl_managed_static_poll_events(
                server, 100000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t k = 0; k < report.copied; k++) {
            /* NOTE: BODY_AVAILABLE reuses method_len for staged-ready bytes and path_len for
               the end flag; see the Rust field-reuse docs. */
            if (events[k].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE &&
                memcmp(&events[k].token, &token, sizeof(token)) == 0) {
                assert(events[k].route_id == 0);
                assert(events[k].method_offset == 0 && events[k].path_offset == 0);
                if (events[k].method_len == sizeof(h2_post_body) - 1 && events[k].path_len == 1) {
                    h2_have_body = 1;
                }
            }
        }
    }
    assert(h2_have_body == 1);
    uint8_t h2staged[8] = {0};
    size_t h2total = 0;
    uint32_t h2body_end = 0;
    for (int i = 0; i < 5000 && h2body_end == 0; i++) {
        uint8_t chunk[8] = {0};
        size_t pulled = 0;
        uint32_t end = 0;
        assert(
            vyl_managed_static_host_pull_body(server, token, chunk, sizeof(chunk), &pulled, &end) ==
            VYL_HTTP1_OK
        );
        assert(h2total + pulled <= sizeof(h2staged));
        memcpy(h2staged + h2total, chunk, pulled);
        h2total += pulled;
        h2body_end = end;
        if (h2body_end == 0) {
            nanosleep(&pause, NULL);
        }
    }
    assert(h2body_end == 1);
    assert(h2total == sizeof(h2_post_body) - 1);
    assert(memcmp(h2staged, "hello", 5) == 0);
    assert(
        vyl_managed_static_host_respond(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            (vyl_http1_bytes){.data = h2_reply, .length = sizeof(h2_reply) - 1}
        ) == VYL_HTTP1_OK
    );
    uint8_t h2resp[16] = {0};
    size_t h2resp_len = 0;
    uint16_t h2status = 0;
    int h2_got_head = 0;
    int h2_got_end = 0;
    for (int i = 0; i < 32 && !h2_got_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 1) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 1, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2status = class.status;
                h2_got_head = 1;
            } else if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                       h2actions[k].stream_id == 1) {
                assert(h2resp_len + h2actions[k].data_length <= sizeof(h2resp));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2resp + h2resp_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2resp_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 1, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2_got_end = 1;
                }
            }
        }
    }
    assert(h2_got_head == 1);
    assert(h2_got_end == 1);
    assert(h2status == 200);
    assert(h2resp_len == sizeof(h2_reply) - 1);
    assert(memcmp(h2resp, "done", 4) == 0);

    /* NOTE: multi-DATA BODY_AVAILABLE on stream 3: the first chunk stages ready bytes
       without the end flag, the second grows the ready count and sets it; pull_body
       then drains both chunks as one exact body. */
    static const uint8_t h2_first[] = "he";
    static const uint8_t h2_second[] = "llo";
    uint32_t h2multi_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2req, sizeof(h2req) / sizeof(h2req[0]), 0, h2wire, sizeof(h2wire), &h2written,
            &h2multi_id, &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2multi_id == 3);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    assert(
        vyl_managed_static_poll_events(
            server, 5000000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths), &report
        ) == VYL_HTTP1_OK
    );
    int h2multi_request = 0;
    for (size_t k = 0; k < report.copied; k++) {
        if (events[k].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_REQUEST &&
            events[k].token.stream_id == 3) {
            assert(events[k].method_len == sizeof(post_method) - 1);
            assert(memcmp(methods + events[k].method_offset, post_method, events[k].method_len) == 0);
            assert(events[k].path_len == sizeof(u_path) - 1);
            assert(memcmp(paths + events[k].path_offset, u_path, events[k].path_len) == 0);
            token = events[k].token;
            h2multi_request = 1;
        }
    }
    assert(h2multi_request == 1);
    assert(
        vyl_h2_client_send_data(
            h2, 3,
            (vyl_http1_bytes){.data = h2_first, .length = sizeof(h2_first) - 1}, 0, h2wire,
            sizeof(h2wire), &h2consumed, &h2written, &h2end, &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2consumed == sizeof(h2_first) - 1);
    assert(h2end == 0);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    int h2_have_first = 0;
    for (int i = 0; i < 500 && !h2_have_first; i++) {
        assert(
            vyl_managed_static_poll_events(
                server, 100000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t k = 0; k < report.copied; k++) {
            /* NOTE: BODY_AVAILABLE reuses method_len for staged-ready bytes and path_len for
               the end flag; see the Rust field-reuse docs. */
            if (events[k].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE &&
                memcmp(&events[k].token, &token, sizeof(token)) == 0) {
                assert(events[k].route_id == 0);
                assert(events[k].method_offset == 0 && events[k].path_offset == 0);
                if (events[k].method_len == sizeof(h2_first) - 1 && events[k].path_len == 0) {
                    h2_have_first = 1;
                }
            }
        }
    }
    assert(h2_have_first == 1);
    assert(
        vyl_h2_client_send_data(
            h2, 3,
            (vyl_http1_bytes){.data = h2_second, .length = sizeof(h2_second) - 1},
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2consumed, &h2written, &h2end,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2consumed == sizeof(h2_second) - 1);
    assert(h2end == (uint32_t)VYL_H2_END_STREAM);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    int h2_have_rest = 0;
    for (int i = 0; i < 500 && !h2_have_rest; i++) {
        assert(
            vyl_managed_static_poll_events(
                server, 100000000u, events, 8, methods, sizeof(methods), paths, sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t k = 0; k < report.copied; k++) {
            if (events[k].kind == (uint32_t)VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE &&
                memcmp(&events[k].token, &token, sizeof(token)) == 0) {
                assert(events[k].route_id == 0);
                assert(events[k].method_offset == 0 && events[k].path_offset == 0);
                if (events[k].method_len == sizeof(h2_post_body) - 1 && events[k].path_len == 1) {
                    h2_have_rest = 1;
                }
            }
        }
    }
    assert(h2_have_rest == 1);
    memset(h2staged, 0, sizeof(h2staged));
    h2total = 0;
    h2body_end = 0;
    for (int i = 0; i < 5000 && h2body_end == 0; i++) {
        uint8_t chunk[8] = {0};
        size_t pulled = 0;
        uint32_t end = 0;
        assert(
            vyl_managed_static_host_pull_body(server, token, chunk, sizeof(chunk), &pulled, &end) ==
            VYL_HTTP1_OK
        );
        assert(h2total + pulled <= sizeof(h2staged));
        memcpy(h2staged + h2total, chunk, pulled);
        h2total += pulled;
        h2body_end = end;
        if (h2body_end == 0) {
            nanosleep(&pause, NULL);
        }
    }
    assert(h2body_end == 1);
    assert(h2total == sizeof(h2_post_body) - 1);
    assert(memcmp(h2staged, "hello", 5) == 0);
    assert(
        vyl_managed_static_host_respond(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            (vyl_http1_bytes){.data = h2_reply, .length = sizeof(h2_reply) - 1}
        ) == VYL_HTTP1_OK
    );
    memset(h2resp, 0, sizeof(h2resp));
    h2resp_len = 0;
    h2status = 0;
    h2_got_head = 0;
    h2_got_end = 0;
    for (int i = 0; i < 32 && !h2_got_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 3) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 3, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2status = class.status;
                h2_got_head = 1;
            } else if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                       h2actions[k].stream_id == 3) {
                assert(h2resp_len + h2actions[k].data_length <= sizeof(h2resp));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2resp + h2resp_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2resp_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 3, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2_got_end = 1;
                }
            }
        }
    }
    assert(h2_got_head == 1);
    assert(h2_got_end == 1);
    assert(h2status == 200);
    assert(h2resp_len == sizeof(h2_reply) - 1);
    assert(memcmp(h2resp, "done", 4) == 0);
    assert(vyl_h2_client_destroy(h2) == VYL_HTTP1_OK);
    assert(close(h2fd) == 0);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == 5);
    assert(counts.peer_closed == 1);
    assert(
        counts.completed + counts.peer_closed + counts.reset + counts.timeout +
            counts.protocol_error + counts.response_overflow + counts.transport_error +
            counts.shutdown == 6
    );
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    return 0;
}
