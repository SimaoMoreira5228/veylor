#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
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

 
static void h2_send_raw_headers(int fd, uint32_t stream, const uint8_t* block, size_t block_len) {
    uint8_t head[9] = {0};
    assert(block_len < (1u << 24));
    head[0] = (uint8_t)(block_len >> 16);
    head[1] = (uint8_t)(block_len >> 8);
    head[2] = (uint8_t)block_len;
    head[3] = 1;
    head[4] = 0x05;
    head[5] = (uint8_t)(stream >> 24 & 0x7Fu);
    head[6] = (uint8_t)(stream >> 16);
    head[7] = (uint8_t)(stream >> 8);
    head[8] = (uint8_t)stream;
    h2_send_all(fd, head, sizeof(head));
    h2_send_all(fd, block, block_len);
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
    uint16_t h2c_port = 0;
    assert(vyl_managed_static_h2c_addr(server, &h2c_port) == VYL_HTTP1_OK);
    assert(h2c_port != 0);
    assert(h2c_port != port);

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
    assert_h1_date_shape(response);

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

    {
        static const char nomatch_request[] =
            "POST /hello HTTP/1.1\r\nHost: test\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
        int nfd = connect_loopback(port);
        const struct timeval nomatch_timeout = {.tv_sec = 5, .tv_usec = 0};
        assert(setsockopt(nfd, SOL_SOCKET, SO_RCVTIMEO, &nomatch_timeout, sizeof(nomatch_timeout)) == 0);
        assert(send(nfd, nomatch_request, sizeof(nomatch_request) - 1, 0) == (ssize_t)(sizeof(nomatch_request) - 1));
        total = 0;
        for (;;) {
            received = recv(nfd, response + total, sizeof(response) - 1 - total, 0);
            assert(received >= 0);
            if (received == 0) {
                break;
            }
            total += (size_t)received;
            assert(total < sizeof(response) - 1);
        }
        assert(close(nfd) == 0);
        response[total] = '\0';
        assert(strstr(response, "HTTP/1.1 405 Method Not Allowed\r\n") != NULL);
        assert(strstr(response, "Connection: close\r\n") != NULL);
        assert_h1_date_shape(response);
        {
            const char* head_end = strstr(response, "\r\n\r\n");
            assert(head_end != NULL);
            size_t allow_lines = 0;
            const char* line = response;
            for (;;) {
                const char* next = strstr(line, "\r\n");
                assert(next != NULL);
                size_t len = (size_t)(next - line);
                if (len >= 6 && memcmp(line, "Allow:", 6) == 0) {
                    allow_lines++;
                    assert(len == sizeof("Allow: GET") - 1);
                    assert(memcmp(line, "Allow: GET", sizeof("Allow: GET") - 1) == 0);
                }
                if (next >= head_end) {
                    break;
                }
                line = next + 2;
            }
            assert(allow_lines == 1);
            assert(strcmp(head_end + 4, "Method Not Allowed") == 0);
        }
    }

    /* NOTE: exactly one 8192-byte input cap of bare request line (no headers yet):
       the full buffer with no newline pins the overflow to the target side for a
       414; the server consumes every byte sent, so the close reads back clean EOF
       instead of racing the unread-tail RST a full-send form risks. */
    {
        static char long_req[8192] = {0};
        int lhead = snprintf(long_req, sizeof(long_req), "GET /");
        assert(lhead > 0);
        size_t lfill = sizeof(long_req) - (size_t)lhead;
        memset(long_req + lhead, 'a', lfill);
        int lfd = connect_loopback(port);
        const struct timeval long_timeout = {.tv_sec = 5, .tv_usec = 0};
        assert(setsockopt(lfd, SOL_SOCKET, SO_RCVTIMEO, &long_timeout, sizeof(long_timeout)) == 0);
        size_t lsent = 0;
        while (lsent < sizeof(long_req)) {
            ssize_t written = send(lfd, long_req + lsent, sizeof(long_req) - lsent, 0);
            assert(written > 0);
            lsent += (size_t)written;
        }
        total = 0;
        for (;;) {
            received = recv(lfd, response + total, sizeof(response) - 1 - total, 0);
            assert(received >= 0);
            if (received == 0) {
                break;
            }
            total += (size_t)received;
            assert(total < sizeof(response) - 1);
        }
        assert(close(lfd) == 0);
        response[total] = '\0';
        assert(strstr(response, "HTTP/1.1 414 URI Too Long\r\n") != NULL);
        assert(strstr(response, "Connection: close\r\n") != NULL);
        assert_h1_date_shape(response);
        {
            const char* head_end = strstr(response, "\r\n\r\n");
            assert(head_end != NULL);
            assert(strcmp(head_end + 4, "URI Too Long") == 0);
        }
    }

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
    assert_h1_date_shape(response);

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

    /* NOTE: the mount replays through the start-time rebind (worker_count 2 plus H2C
       stage), so the H1 file cases above already prove the replay; this wire case
       proves the same mount serves exact bytes over H2C through the client handle. */
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
        h2_text_field(":path", "/assets/hello.txt"),
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
                {
                    int h2_has_lm = 0;
                    for (size_t f = h2actions[k].field_start;
                         f < h2actions[k].field_start + h2actions[k].field_count; f++) {
                        assert(f < 64);
                        if (h2fields[f].name_length == 13 &&
                            memcmp(h2data + h2fields[f].name_offset, "last-modified", 13) == 0) {
                            h2_has_lm = 1;
                        }
                    }
                    assert(h2_has_lm == 1);
                }
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
    assert(h2content_length == sizeof(mount_file_body) - 1);
    assert(h2resp_len == sizeof(mount_file_body) - 1);
    assert(memcmp(h2resp, mount_file_body, sizeof(mount_file_body) - 1) == 0);

    /* NOTE: multi-range over the same H2C conn frames one 206 multipart body
       byte-identical to H1: boundary echoes from content-type, parts carry
       exact slices, framed length matches. Subset (one survivor) collapses to
       single 206; a plain sibling after proves the conn survives. */
    const vyl_h2_field_to_encode h2multi_get[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/assets/hello.txt"),
        h2_text_field(":authority", "localhost"),
        h2_text_field("range", "bytes=0-4,6-10"),
    };
    uint32_t h2multi_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2multi_get, sizeof(h2multi_get) / sizeof(h2multi_get[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2multi_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2multi_id == 3);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    static uint8_t h2multi[2048] = {0};
    size_t h2multi_len = 0;
    uint16_t h2multi_status = 0;
    uint64_t h2multi_framed = 0;
    int h2multi_head = 0;
    int h2multi_end = 0;
    static uint8_t h2ctype[128] = {0};
    size_t h2ctype_len = 0;
    int h2multi_crange = 0;
    for (int i = 0; i < 32 && !h2multi_end; i++) {
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
                h2multi_status = class.status;
                h2multi_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                for (size_t f = h2actions[k].field_start;
                     f < h2actions[k].field_start + h2actions[k].field_count; f++) {
                    assert(f < 64);
                    if (h2fields[f].name_length == 12 &&
                        memcmp(h2data + h2fields[f].name_offset, "content-type", 12) == 0) {
                        assert(h2fields[f].value_length < sizeof(h2ctype));
                        memcpy(
                            h2ctype, h2data + h2fields[f].value_offset,
                            h2fields[f].value_length
                        );
                        h2ctype_len = h2fields[f].value_length;
                    }
                    if (h2fields[f].name_length == 13 &&
                        memcmp(h2data + h2fields[f].name_offset, "content-range", 13) == 0) {
                        h2multi_crange = 1;
                    }
                }
                h2multi_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2multi_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 3
            ) {
                assert(h2multi_len + h2actions[k].data_length <= sizeof(h2multi));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2multi + h2multi_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2multi_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 3, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2multi_end = 1;
                }
            }
        }
    }
    assert(h2multi_head == 1);
    assert(h2multi_end == 1);
    assert(h2multi_status == 206);
    assert(h2multi_crange == 0);
    static const char h2multi_prefix[] = "multipart/byteranges; boundary=";
    assert(h2ctype_len > sizeof(h2multi_prefix) - 1);
    assert(memcmp(h2ctype, h2multi_prefix, sizeof(h2multi_prefix) - 1) == 0);
    const uint8_t* h2boundary = h2ctype + sizeof(h2multi_prefix) - 1;
    size_t h2boundary_len = h2ctype_len - (sizeof(h2multi_prefix) - 1);
    static const char h2veyor_prefix[] = "VEYLOR_";
    assert(h2boundary_len > sizeof(h2veyor_prefix) - 1);
    assert(memcmp(h2boundary, h2veyor_prefix, sizeof(h2veyor_prefix) - 1) == 0);
    static uint8_t h2expected[2048] = {0};
    int h2expected_len = snprintf(
        (char*)h2expected, sizeof(h2expected),
        "--%.*s\r\nContent-Type: text/plain\r\nContent-Range: bytes 0-4/11\r\n\r\n"
        "mount\r\n--%.*s\r\nContent-Type: text/plain\r\nContent-Range: bytes 6-10/11\r\n\r\n"
        "bytes\r\n--%.*s--\r\n",
        (int)h2boundary_len, (const char*)h2boundary, (int)h2boundary_len,
        (const char*)h2boundary, (int)h2boundary_len, (const char*)h2boundary
    );
    assert(h2expected_len > 0 && (size_t)h2expected_len < sizeof(h2expected));
    assert(h2multi_framed == (uint64_t)h2expected_len);
    assert(h2multi_len == (size_t)h2expected_len);
    assert(memcmp(h2multi, h2expected, h2multi_len) == 0);

    const vyl_h2_field_to_encode h2subset_get[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/assets/hello.txt"),
        h2_text_field(":authority", "localhost"),
        h2_text_field("range", "bytes=0-4,99-"),
    };
    uint32_t h2subset_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2subset_get, sizeof(h2subset_get) / sizeof(h2subset_get[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2subset_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2subset_id == 5);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint8_t h2subset[16] = {0};
    size_t h2subset_len = 0;
    uint16_t h2subset_status = 0;
    uint64_t h2subset_framed = 0;
    int h2subset_head = 0;
    int h2subset_end = 0;
    static uint8_t h2subset_ctype[32] = {0};
    size_t h2subset_ctype_len = 0;
    static uint8_t h2subset_crange[32] = {0};
    size_t h2subset_crange_len = 0;
    for (int i = 0; i < 32 && !h2subset_end; i++) {
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
                h2subset_status = class.status;
                h2subset_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                for (size_t f = h2actions[k].field_start;
                     f < h2actions[k].field_start + h2actions[k].field_count; f++) {
                    assert(f < 64);
                    if (h2fields[f].name_length == 12 &&
                        memcmp(h2data + h2fields[f].name_offset, "content-type", 12) == 0) {
                        assert(h2fields[f].value_length < sizeof(h2subset_ctype));
                        memcpy(
                            h2subset_ctype, h2data + h2fields[f].value_offset,
                            h2fields[f].value_length
                        );
                        h2subset_ctype_len = h2fields[f].value_length;
                    }
                    if (h2fields[f].name_length == 13 &&
                        memcmp(h2data + h2fields[f].name_offset, "content-range", 13) == 0) {
                        assert(h2fields[f].value_length < sizeof(h2subset_crange));
                        memcpy(
                            h2subset_crange, h2data + h2fields[f].value_offset,
                            h2fields[f].value_length
                        );
                        h2subset_crange_len = h2fields[f].value_length;
                    }
                }
                h2subset_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2subset_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 5
            ) {
                assert(h2subset_len + h2actions[k].data_length <= sizeof(h2subset));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2subset + h2subset_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2subset_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 5, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2subset_end = 1;
                }
            }
        }
    }
    assert(h2subset_head == 1);
    assert(h2subset_end == 1);
    assert(h2subset_status == 206);
    assert(h2subset_framed == 5);
    assert(h2subset_len == 5);
    assert(memcmp(h2subset, "mount", 5) == 0);
    assert(h2subset_ctype_len == sizeof("text/plain") - 1);
    assert(memcmp(h2subset_ctype, "text/plain", sizeof("text/plain") - 1) == 0);
    assert(h2subset_crange_len == sizeof("bytes 0-4/11") - 1);
    assert(memcmp(h2subset_crange, "bytes 0-4/11", sizeof("bytes 0-4/11") - 1) == 0);

    const vyl_h2_field_to_encode h2sib_get[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/assets/hello.txt"),
        h2_text_field(":authority", "localhost"),
    };
    uint32_t h2sib_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2sib_get, sizeof(h2sib_get) / sizeof(h2sib_get[0]), (uint32_t)VYL_H2_END_STREAM,
            h2wire, sizeof(h2wire), &h2written, &h2sib_id, &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2sib_id == 7);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint8_t h2sib[16] = {0};
    size_t h2sib_len = 0;
    uint16_t h2sib_status = 0;
    uint64_t h2sib_framed = 0;
    int h2sib_head = 0;
    int h2sib_end = 0;
    for (int i = 0; i < 32 && !h2sib_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 7) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 7, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2sib_status = class.status;
                h2sib_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                {
                    int h2sib_has_lm = 0;
                    for (size_t f = h2actions[k].field_start;
                         f < h2actions[k].field_start + h2actions[k].field_count; f++) {
                        assert(f < 64);
                        if (h2fields[f].name_length == 13 &&
                            memcmp(h2data + h2fields[f].name_offset, "last-modified", 13) == 0) {
                            h2sib_has_lm = 1;
                        }
                    }
                    assert(h2sib_has_lm == 1);
                }
                h2sib_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2sib_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 7
            ) {
                assert(h2sib_len + h2actions[k].data_length <= sizeof(h2sib));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2sib + h2sib_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2sib_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 7, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2sib_end = 1;
                }
            }
        }
    }
    assert(h2sib_head == 1);
    assert(h2sib_end == 1);
    assert(h2sib_status == 200);
    assert(h2sib_framed == sizeof(mount_file_body) - 1);
    assert(h2sib_len == sizeof(mount_file_body) - 1);
    assert(memcmp(h2sib, mount_file_body, sizeof(mount_file_body) - 1) == 0);

    /* NOTE: static GET over the same H2C conn pins Date first + 29-byte IMF-fixdate
       shape on a non-file HEADERS (send_full emits :status, date, content-length). */
    const vyl_h2_field_to_encode h2static_get[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/hello"),
        h2_text_field(":authority", "localhost"),
    };
    uint32_t h2static_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2static_get, sizeof(h2static_get) / sizeof(h2static_get[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2static_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2static_id == 9);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint8_t h2static_resp[8] = {0};
    size_t h2static_len = 0;
    uint16_t h2static_status = 0;
    uint64_t h2static_framed = 0;
    int h2static_head = 0;
    int h2static_end = 0;
    for (int i = 0; i < 32 && !h2static_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 9) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 9, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2static_status = class.status;
                h2static_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                h2static_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2static_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 9
            ) {
                assert(h2static_len + h2actions[k].data_length <= sizeof(h2static_resp));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2static_resp + h2static_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2static_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 9, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2static_end = 1;
                }
            }
        }
    }
    assert(h2static_head == 1);
    assert(h2static_end == 1);
    assert(h2static_status == 200);
    assert(h2static_framed == sizeof(body) - 1);
    assert(h2static_len == sizeof(body) - 1);
    assert(memcmp(h2static_resp, body, sizeof(body) - 1) == 0);

    /* NOTE: conditional GET over the same conn reuses the H1 last-modified: 304
       completes with HEADERS alone (date + last-modified, no body). */
    const vyl_h2_field_to_encode h2cond_get[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/assets/hello.txt"),
        h2_text_field(":authority", "localhost"),
        h2_text_field("if-modified-since", last_modified),
    };
    uint32_t h2cond_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2cond_get, sizeof(h2cond_get) / sizeof(h2cond_get[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2cond_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2cond_id == 11);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint16_t h2cond_status = 0;
    int h2cond_head = 0;
    int h2cond_end = 0;
    for (int i = 0; i < 32 && !h2cond_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 11) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 11, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2cond_status = class.status;
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                {
                    int h2cond_has_lm = 0;
                    for (size_t f = h2actions[k].field_start;
                         f < h2actions[k].field_start + h2actions[k].field_count; f++) {
                        assert(f < 64);
                        if (h2fields[f].name_length == 13 &&
                            memcmp(h2data + h2fields[f].name_offset, "last-modified", 13) == 0) {
                            h2cond_has_lm = 1;
                        }
                    }
                    assert(h2cond_has_lm == 1);
                }
                h2cond_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2cond_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 11
            ) {
                assert(0 && "304 carries no body");
            }
        }
    }
    assert(h2cond_head == 1);
    assert(h2cond_end == 1);
    assert(h2cond_status == 304);

    /* NOTE: unsatisfiable range over the same conn fails closed as 416 with HEADERS
       alone (date plus content-range bytes-star/11, no body). */
    const vyl_h2_field_to_encode h2unsat_get[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/assets/hello.txt"),
        h2_text_field(":authority", "localhost"),
        h2_text_field("range", "bytes=99-"),
    };
    uint32_t h2unsat_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2unsat_get, sizeof(h2unsat_get) / sizeof(h2unsat_get[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2unsat_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2unsat_id == 13);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint16_t h2unsat_status = 0;
    uint64_t h2unsat_framed = 0;
    int h2unsat_head = 0;
    int h2unsat_end = 0;
    static uint8_t h2unsat_crange[32] = {0};
    size_t h2unsat_crange_len = 0;
    for (int i = 0; i < 32 && !h2unsat_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 13) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 13, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2unsat_status = class.status;
                h2unsat_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                for (size_t f = h2actions[k].field_start;
                     f < h2actions[k].field_start + h2actions[k].field_count; f++) {
                    assert(f < 64);
                    if (h2fields[f].name_length == 13 &&
                        memcmp(h2data + h2fields[f].name_offset, "content-range", 13) == 0) {
                        assert(h2fields[f].value_length < sizeof(h2unsat_crange));
                        memcpy(
                            h2unsat_crange, h2data + h2fields[f].value_offset,
                            h2fields[f].value_length
                        );
                        h2unsat_crange_len = h2fields[f].value_length;
                    }
                }
                h2unsat_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2unsat_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 13
            ) {
                assert(0 && "416 carries no body");
            }
        }
    }
    assert(h2unsat_head == 1);
    assert(h2unsat_end == 1);
    assert(h2unsat_status == 416);
    assert(h2unsat_framed == 0);
    assert(h2unsat_crange_len == sizeof("bytes */11") - 1);
    assert(memcmp(h2unsat_crange, "bytes */11", sizeof("bytes */11") - 1) == 0);

    /* NOTE: POST on the GET-only static answers 405 with the exact registered methods
       in one allow field plus the 18-byte body; a sibling GET after proves the conn
       survives the per-stream refusal. */
    const vyl_h2_field_to_encode h2deny_post[] = {
        h2_text_field(":method", "POST"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/hello"),
        h2_text_field(":authority", "localhost"),
    };
    uint32_t h2deny_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2deny_post, sizeof(h2deny_post) / sizeof(h2deny_post[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2deny_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2deny_id == 15);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint8_t h2deny_resp[32] = {0};
    size_t h2deny_len = 0;
    uint16_t h2deny_status = 0;
    uint64_t h2deny_framed = 0;
    int h2deny_head = 0;
    int h2deny_end = 0;
    for (int i = 0; i < 32 && !h2deny_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 15) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 15, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2deny_status = class.status;
                h2deny_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                {
                    int h2deny_allow = 0;
                    for (size_t f = h2actions[k].field_start;
                         f < h2actions[k].field_start + h2actions[k].field_count; f++) {
                        assert(f < 64);
                        if (h2fields[f].name_length == 5 &&
                            memcmp(h2data + h2fields[f].name_offset, "allow", 5) == 0) {
                            assert(h2fields[f].value_length == sizeof("GET") - 1);
                            assert(
                                memcmp(
                                    h2data + h2fields[f].value_offset, "GET",
                                    sizeof("GET") - 1
                                ) == 0
                            );
                            h2deny_allow++;
                        }
                    }
                    assert(h2deny_allow == 1);
                }
                h2deny_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2deny_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 15
            ) {
                assert(h2deny_len + h2actions[k].data_length <= sizeof(h2deny_resp));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2deny_resp + h2deny_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2deny_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 15, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2deny_end = 1;
                }
            }
        }
    }
    assert(h2deny_head == 1);
    assert(h2deny_end == 1);
    assert(h2deny_status == 405);
    assert(h2deny_framed == sizeof("Method Not Allowed") - 1);
    assert(h2deny_len == sizeof("Method Not Allowed") - 1);
    assert(memcmp(h2deny_resp, "Method Not Allowed", sizeof("Method Not Allowed") - 1) == 0);

    const vyl_h2_field_to_encode h2deny_sib[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/hello"),
        h2_text_field(":authority", "localhost"),
    };
    uint32_t h2deny_sib_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2deny_sib, sizeof(h2deny_sib) / sizeof(h2deny_sib[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2deny_sib_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2deny_sib_id == 17);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint8_t h2deny_sib_resp[8] = {0};
    size_t h2deny_sib_len = 0;
    uint16_t h2deny_sib_status = 0;
    uint64_t h2deny_sib_framed = 0;
    int h2deny_sib_head = 0;
    int h2deny_sib_end = 0;
    for (int i = 0; i < 32 && !h2deny_sib_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 17) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 17, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2deny_sib_status = class.status;
                h2deny_sib_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                h2deny_sib_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2deny_sib_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 17
            ) {
                assert(h2deny_sib_len + h2actions[k].data_length <= sizeof(h2deny_sib_resp));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2deny_sib_resp + h2deny_sib_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2deny_sib_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 17, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2deny_sib_end = 1;
                }
            }
        }
    }
    assert(h2deny_sib_head == 1);
    assert(h2deny_sib_end == 1);
    assert(h2deny_sib_status == 200);
    assert(h2deny_sib_framed == sizeof(body) - 1);
    assert(h2deny_sib_len == sizeof(body) - 1);
    assert(memcmp(h2deny_sib_resp, body, sizeof(body) - 1) == 0);

    /* NOTE: over-long :path over the same H2C conn answers 414 with Date first plus the
       12-byte body (HEADERS then DATA, never the 404 the miss used to give); a sibling
       GET after proves the conn survives the per-stream refusal. The 9 KiB literal
       cannot stage through the client handle's 4 KiB encode scratch, so the single
       HEADERS frame goes out raw in the dispatch.c literal-without-indexing byte
       pattern while a phantom same-method client request registers stream 19 for the
       standard classify/account response path (its bytes are acked, never sent, and
       without-indexing literals drift no HPACK table on either side). Single frame
       mirrors long_path_answers_414_and_connection_survives. */
    static char h2long_path[9002] = {0};
    h2long_path[0] = '/';
    memset(h2long_path + 1, 'a', sizeof(h2long_path) - 2);
    assert(strlen(h2long_path) == 9001);
    assert(127 + 42 + 69 * 128 == 9001);
    static const uint8_t h2long_method[] = {
        0x00, 0x07, ':', 'm', 'e', 't', 'h', 'o', 'd', 0x03, 'G', 'E', 'T',
    };
    static const uint8_t h2long_scheme[] = {
        0x00, 0x07, ':', 's', 'c', 'h', 'e', 'm', 'e', 0x04, 'h', 't', 't', 'p',
    };
    static const uint8_t h2long_path_head[] = {
        0x00, 0x05, ':', 'p', 'a', 't', 'h', 0x7f, 0xaa, 0x45,
    };
    static const uint8_t h2long_auth[] = {
        0x00, 0x0a, ':', 'a', 'u', 't', 'h', 'o', 'r', 'i', 't', 'y', 0x09, 'l', 'o', 'c',
        'a', 'l', 'h', 'o', 's', 't',
    };
    static uint8_t h2long_block[9060] = {0};
    size_t h2long_off = 0;
    assert(sizeof(h2long_method) == 13);
    memcpy(h2long_block + h2long_off, h2long_method, sizeof(h2long_method));
    h2long_off += sizeof(h2long_method);
    assert(sizeof(h2long_scheme) == 14);
    memcpy(h2long_block + h2long_off, h2long_scheme, sizeof(h2long_scheme));
    h2long_off += sizeof(h2long_scheme);
    assert(sizeof(h2long_path_head) == 10);
    memcpy(h2long_block + h2long_off, h2long_path_head, sizeof(h2long_path_head));
    h2long_off += sizeof(h2long_path_head);
    assert(h2long_off + sizeof(h2long_path) - 1 + sizeof(h2long_auth) == sizeof(h2long_block));
    memcpy(h2long_block + h2long_off, h2long_path, sizeof(h2long_path) - 1);
    h2long_off += sizeof(h2long_path) - 1;
    memcpy(h2long_block + h2long_off, h2long_auth, sizeof(h2long_auth));
    h2long_off += sizeof(h2long_auth);
    assert(h2long_off == sizeof(h2long_block));
    const vyl_h2_field_to_encode h2long_phantom[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/hello"),
        h2_text_field(":authority", "localhost"),
    };
    uint32_t h2long_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2long_phantom, sizeof(h2long_phantom) / sizeof(h2long_phantom[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2long_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2long_id == 19);
    h2_ack_client(h2, h2written);
    h2_send_raw_headers(h2fd, 19, h2long_block, sizeof(h2long_block));
    uint8_t h2long_resp[16] = {0};
    size_t h2long_len = 0;
    uint16_t h2long_status = 0;
    uint64_t h2long_framed = 0;
    int h2long_head = 0;
    int h2long_end = 0;
    for (int i = 0; i < 32 && !h2long_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 19) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 19, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2long_status = class.status;
                h2long_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                h2long_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2long_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 19
            ) {
                assert(h2long_len + h2actions[k].data_length <= sizeof(h2long_resp));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2long_resp + h2long_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2long_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 19, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2long_end = 1;
                }
            }
        }
    }
    assert(h2long_head == 1);
    assert(h2long_end == 1);
    assert(h2long_status == 414);
    assert(h2long_framed == sizeof("URI Too Long") - 1);
    assert(h2long_len == sizeof("URI Too Long") - 1);
    assert(memcmp(h2long_resp, "URI Too Long", sizeof("URI Too Long") - 1) == 0);

    const vyl_h2_field_to_encode h2long_sib[] = {
        h2_text_field(":method", "GET"),
        h2_text_field(":scheme", "http"),
        h2_text_field(":path", "/hello"),
        h2_text_field(":authority", "localhost"),
    };
    uint32_t h2long_sib_id = 0;
    assert(
        vyl_h2_client_start_request(
            h2, h2long_sib, sizeof(h2long_sib) / sizeof(h2long_sib[0]),
            (uint32_t)VYL_H2_END_STREAM, h2wire, sizeof(h2wire), &h2written, &h2long_sib_id,
            &h2required
        ) == VYL_HTTP1_OK
    );
    assert(h2long_sib_id == 21);
    h2_send_all(h2fd, h2wire, h2written);
    h2_ack_client(h2, h2written);
    uint8_t h2long_sib_resp[8] = {0};
    size_t h2long_sib_len = 0;
    uint16_t h2long_sib_status = 0;
    uint64_t h2long_sib_framed = 0;
    int h2long_sib_head = 0;
    int h2long_sib_end = 0;
    for (int i = 0; i < 32 && !h2long_sib_end; i++) {
        size_t frame_length =
            h2_read_frame(h2fd, h2frame, sizeof(h2frame), &h2type, &h2flags, &h2stream);
        h2_feed_client(h2, h2frame, frame_length, h2actions, h2fields, h2data, &h2got, &h2seen);
        for (size_t k = 0; k < h2got; k++) {
            if (h2actions[k].kind == (uint32_t)VYL_H2_ACTION_HEADERS &&
                h2actions[k].stream_id == 21) {
                vyl_h2_response_class class = {0};
                assert(
                    vyl_h2_client_classify_headers(
                        h2, 21, h2actions[k].field_start, h2actions[k].field_count,
                        h2actions[k].flags, &class
                    ) == VYL_HTTP1_OK
                );
                assert(class.kind == (uint32_t)VYL_H2_RESPONSE_FINAL);
                h2long_sib_status = class.status;
                h2long_sib_framed = class.content_length;
                assert(class.has_content_length == 1);
                assert_h2_date_shape(
                    h2fields, h2actions[k].field_start, h2actions[k].field_count, h2data
                );
                h2long_sib_head = 1;
                if ((h2actions[k].flags & 1u) != 0) {
                    h2long_sib_end = 1;
                }
            } else if (
                h2actions[k].kind == (uint32_t)VYL_H2_ACTION_DATA &&
                h2actions[k].stream_id == 21
            ) {
                assert(h2long_sib_len + h2actions[k].data_length <= sizeof(h2long_sib_resp));
                assert(h2actions[k].data_offset + h2actions[k].data_length <= sizeof(h2data));
                memcpy(
                    h2long_sib_resp + h2long_sib_len, h2data + h2actions[k].data_offset,
                    h2actions[k].data_length
                );
                h2long_sib_len += h2actions[k].data_length;
                uint32_t phase = 0;
                assert(
                    vyl_h2_client_account_data(
                        h2, 21, h2actions[k].data_length, h2actions[k].flags, &phase
                    ) == VYL_HTTP1_OK
                );
                if ((h2actions[k].flags & 1u) != 0) {
                    assert(phase == (uint32_t)VYL_H2_PHASE_COMPLETE);
                    h2long_sib_end = 1;
                }
            }
        }
    }
    assert(h2long_sib_head == 1);
    assert(h2long_sib_end == 1);
    assert(h2long_sib_status == 200);
    assert(h2long_sib_framed == sizeof(body) - 1);
    assert(h2long_sib_len == sizeof(body) - 1);
    assert(memcmp(h2long_sib_resp, body, sizeof(body) - 1) == 0);

    assert(vyl_h2_client_destroy(h2) == VYL_HTTP1_OK);
    assert(close(h2fd) == 0);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == 6);
    assert(counts.peer_closed == 1);
    assert(counts.protocol_error == 1);
    assert(
        counts.completed + counts.peer_closed + counts.reset + counts.timeout +
            counts.protocol_error + counts.response_overflow + counts.transport_error +
            counts.shutdown == 8
    );
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    assert(remove(mount_path) == 0);
    assert(rmdir(mount_dir) == 0);
    return 0;
}
