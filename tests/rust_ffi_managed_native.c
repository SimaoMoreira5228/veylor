#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

_Static_assert(
    sizeof(vyl_managed_static_native_request) == 56,
    "native request twin must match Rust NativeRequestView"
);
_Static_assert(
    offsetof(vyl_managed_static_native_request, body) == 48,
    "native request body offset must match Rust"
);
_Static_assert(
    sizeof(vyl_managed_static_native_response) == 40,
    "native response twin must match Rust NativeResponseOut"
);

static int native_tag = 0x5EED;

static int32_t native_handler(
    void* user_data,
    const vyl_managed_static_native_request* request,
    vyl_managed_static_native_response* response
) {
    static const uint8_t reason[] = "OK";
    static const uint8_t body[] = "native-hello";
    assert(user_data == &native_tag);
    assert(request->method_len == 3);
    assert(memcmp(request->method, "GET", 3) == 0);
    assert(request->path_len == 7);
    assert(memcmp(request->path, "/native", 7) == 0);
    assert(request->has_body == 0);
    assert(request->body_len == 0);
    assert(request->body == NULL);
    response->status = 200;
    response->reason = reason;
    response->reason_len = sizeof(reason) - 1;
    response->body = body;
    response->body_len = sizeof(body) - 1;
    return 0;
}

static int32_t native_echo(
    void* user_data,
    const vyl_managed_static_native_request* request,
    vyl_managed_static_native_response* response
) {
    static const uint8_t reason[] = "OK";
    assert(user_data == &native_tag);
    assert(request->method_len == 4);
    assert(memcmp(request->method, "POST", 4) == 0);
    assert(request->path_len == 5);
    assert(memcmp(request->path, "/echo", 5) == 0);
    assert(request->has_body == 1);
    assert(request->body_len == 8);
    assert(request->body != NULL);
    assert(memcmp(request->body, "echo-me!", 8) == 0);
    response->status = 200;
    response->reason = reason;
    response->reason_len = sizeof(reason) - 1;
    response->body = request->body;
    response->body_len = (size_t)request->body_len;
    return 0;
}

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

static size_t read_closed(int fd, char* out, size_t cap) {
    size_t total = 0;
    for (;;) {
        ssize_t received = recv(fd, out + total, cap - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < cap - 1);
    }
    out[total] = '\0';
    return total;
}

/* NOTE: H2C helpers are minimal local copies of the rust_ffi_managed_dispatch.c byte
   pattern (file-local statics there, so not reusable): whole frames feed the
   vyl_h2_client_* handle, no second HPACK/framing implementation. */
static void recv_exact(int fd, void* out, size_t length) {
    size_t total = 0;
    while (total < length) {
        ssize_t received = recv(fd, (char*)out + total, length - total, 0);
        assert(received > 0);
        total += (size_t)received;
    }
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

static void h2_feed_client(
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
}

static void assert_h1_date_shape(const char* response) {
    const char* eol = strstr(response, "\r\n");
    assert(eol != NULL);
    assert(strncmp(eol + 2, "Date: ", 6) == 0);
    const char* value = eol + 8;
    assert(strlen(value) >= 31);
    assert(value[3] == ',');
    assert(memcmp(value + 25, " GMT", 4) == 0);
    assert(memcmp(value + 29, "\r\n", 2) == 0);
    const char* head_end = strstr(response, "\r\n\r\n");
    assert(head_end != NULL);
    size_t count = 0;
    const char* line = response;
    for (;;) {
        const char* next = strstr(line, "\r\n");
        size_t len = next != NULL ? (size_t)(next - line) : strlen(line);
        if (len >= 5 && (line[0] == 'D' || line[0] == 'd') &&
            (line[1] == 'A' || line[1] == 'a') && (line[2] == 'T' || line[2] == 't') &&
            (line[3] == 'E' || line[3] == 'e') && line[4] == ':') {
            count++;
        }
        if (next == NULL || next >= head_end) {
            break;
        }
        line = next + 2;
    }
    assert(count == 1);
}

static void assert_h2_date_shape(
    const vyl_h2_field* fields,
    size_t field_start,
    size_t field_count,
    const uint8_t* data
) {
    assert(field_count >= 2);
    assert(fields[field_start].name_length == 7);
    assert(memcmp(data + fields[field_start].name_offset, ":status", 7) == 0);
    assert(fields[field_start + 1].name_length == 4);
    assert(memcmp(data + fields[field_start + 1].name_offset, "date", 4) == 0);
    size_t count = 0;
    for (size_t i = field_start; i < field_start + field_count; i++) {
        if (fields[i].name_length == 4 &&
            memcmp(data + fields[i].name_offset, "date", 4) == 0) {
            assert(fields[i].value_length == 29);
            const uint8_t* value = data + fields[i].value_offset;
            assert(value[3] == ',');
            assert(memcmp(value + 25, " GMT", 4) == 0);
            count++;
        }
    }
    assert(count == 1);
}

int main(void) {
    static const uint8_t method[] = "GET";
    static const uint8_t path[] = "/native";
    static const uint8_t post_method[] = "POST";
    static const uint8_t echo_path[] = "/echo";
    static const char request[] = "GET /native HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    static const char echo_request[] = "POST /echo HTTP/1.1\r\nHost: test\r\nContent-Length: 8\r\nConnection: close\r\n\r\necho-me!";
    vyl_managed_static_config config = {0};
    vyl_managed_static_server* server = NULL;
    vyl_managed_static_close_counts counts = {0};
    uint16_t port = 0;
    int fd = -1;
    char response[1024] = {0};

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
        vyl_managed_static_register_native_route(NULL, (vyl_http1_bytes){0}, (vyl_http1_bytes){0}, native_handler, NULL) ==
            VYL_HTTP1_INVALID_ARGUMENT
    );
    assert(
        vyl_managed_static_register_native_route_with_gzip(NULL, (vyl_http1_bytes){0}, (vyl_http1_bytes){0}, native_handler, NULL) ==
            VYL_HTTP1_INVALID_ARGUMENT
    );
    assert(
        vyl_managed_static_register_native_route_with_gzip(
            server,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = path, .length = sizeof(path) - 1},
            NULL,
            &native_tag
        ) == VYL_HTTP1_INVALID_ARGUMENT
    );
    assert(
        vyl_managed_static_register_native_route(
            server,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = path, .length = sizeof(path) - 1},
            NULL,
            &native_tag
        ) == VYL_HTTP1_INVALID_ARGUMENT
    );
    assert(
        vyl_managed_static_register_native_route(
            server,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = path, .length = sizeof(path) - 1},
            native_handler,
            &native_tag
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_managed_static_register_native_route(
            server,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = path, .length = sizeof(path) - 1},
            native_handler,
            &native_tag
        ) == VYL_HTTP1_INVALID_ARGUMENT
    );
    assert(
        vyl_managed_static_register_native_route(
            server,
            (vyl_http1_bytes){.data = post_method, .length = sizeof(post_method) - 1},
            (vyl_http1_bytes){.data = echo_path, .length = sizeof(echo_path) - 1},
            native_echo,
            &native_tag
        ) == VYL_HTTP1_OK
    );
    assert(vyl_managed_static_enable_h2c(server, 0) == VYL_HTTP1_OK);
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);
    assert(
        vyl_managed_static_register_native_route_with_gzip(
            server,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = path, .length = sizeof(path) - 1},
            native_handler,
            &native_tag
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);
    uint16_t h2c_port = 0;
    assert(vyl_managed_static_h2c_addr(server, &h2c_port) == VYL_HTTP1_OK);
    assert(h2c_port != 0);
    assert(h2c_port != port);

    fd = connect_loopback(port);
    assert(send(fd, request, sizeof(request) - 1, 0) == (ssize_t)(sizeof(request) - 1));
    read_closed(fd, response, sizeof(response));
    assert(close(fd) == 0);
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "native-hello") != NULL);
    assert_h1_date_shape(response);

    fd = connect_loopback(port);
    assert(send(fd, echo_request, sizeof(echo_request) - 1, 0) == (ssize_t)(sizeof(echo_request) - 1));
    read_closed(fd, response, sizeof(response));
    assert(close(fd) == 0);
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "echo-me!") != NULL);

    /* NOTE: native over H2C through the h2c_addr port: handshake, GET /native with
       END_STREAM, exact status/body bytes classified through the client handle. The
       handler asserts user_data == &native_tag, so the H2C invoke proves the
       round-trip beside the H1 flows above. */
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
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        if (h2type == 4 && (h2flags & 1u) != 0) {
            h2_settings_ack = 1;
        }
    }
    assert(h2_settings_ack == 1);

    const vyl_h2_field_to_encode h2get[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/native"),
        h2_text_field(":authority", "localhost"),
    };
    uint32_t h2stream_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2get, sizeof(h2get) / sizeof(h2get[0]), (uint32_t)VYL_H2_END_STREAM, h2wire,
            sizeof(h2wire), &h2written, &h2stream_id, &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2stream_id == 1);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint8_t h2resp[16] = {0};
    size_t h2resp_len = 0;
    uint16_t h2status = 0;
    uint64_t h2content_length = 0;
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
                h2content_length = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                h2_got_head = 1;
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 1
            ) {
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
    assert(h2content_length == sizeof("native-hello") - 1);
    assert(h2resp_len == sizeof("native-hello") - 1);
    assert(memcmp(h2resp, "native-hello", sizeof("native-hello") - 1) == 0);

    /* NOTE: framing lie on stream 3 (content-length 8 with END_STREAM, no DATA) resets
       with PROTOCOL_ERROR. Survival of native_echo's body asserts proves the handler
       was never invoked; the sibling GET on stream 5 proves the conn survives. */
    const vyl_h2_field_to_encode h2lie[] = {
        h2_text_field(":method", "POST"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/echo"),
        h2_text_field(":authority", "localhost"),
        h2_text_field("content-length", "8"),
    };
    uint32_t h2lie_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2lie, sizeof(h2lie) / sizeof(h2lie[0]), (uint32_t)VYL_H2_END_STREAM, h2wire,
            sizeof(h2wire), &h2written, &h2lie_id, &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2lie_id == 3);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    int h2_got_rst = 0;
    for (int i = 0; i < 32 && !h2_got_rst; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_STREAM_RESET &&
                h2actions[k].stream_id == 3) {
                assert(h2actions[k].error_code == 1);
                h2_got_rst = 1;
            }
        }
    }
    assert(h2_got_rst == 1);

    uint32_t h2sib_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2get, sizeof(h2get) / sizeof(h2get[0]), (uint32_t)VYL_H2_END_STREAM, h2wire,
            sizeof(h2wire), &h2written, &h2sib_id, &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2sib_id == 5);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
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
                h2actions[k].stream_id == 5) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 5, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2status = class.status;
                h2_got_head = 1;
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 5
            ) {
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
                        h2, 5, h2actions[k].data_length, h2actions[k].flags, &phase
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
    assert(h2resp_len == sizeof("native-hello") - 1);
    assert(memcmp(h2resp, "native-hello", sizeof("native-hello") - 1) == 0);
    assert(vyl_h2_client_destroy(h2) == VYL_HTTP1_OK);
    assert(close(h2fd) == 0);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == 2);
    assert(counts.peer_closed == 1);
    assert(counts.completed == 2);
    assert(
        counts.completed + counts.peer_closed + counts.reset + counts.timeout +
            counts.protocol_error + counts.response_overflow + counts.transport_error +
            counts.shutdown == 3
    );
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    return 0;
}
