#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

_Static_assert(
    sizeof(vyl_managed_static_native_request) == 48,
    "native request twin must match Rust NativeRequestView"
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
    response->status = 200;
    response->reason = reason;
    response->reason_len = sizeof(reason) - 1;
    response->body = body;
    response->body_len = sizeof(body) - 1;
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

int main(void) {
    static const uint8_t method[] = "GET";
    static const uint8_t path[] = "/native";
    static const char request[] = "GET /native HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
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
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);

    fd = connect_loopback(port);
    assert(send(fd, request, sizeof(request) - 1, 0) == (ssize_t)(sizeof(request) - 1));
    read_closed(fd, response, sizeof(response));
    assert(close(fd) == 0);
    assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
    assert(strstr(response, "native-hello") != NULL);

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
