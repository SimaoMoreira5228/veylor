#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

#include "veylor_http1.h"

static const uint8_t HANDSHAKE[] =
    "GET /ws HTTP/1.1\r\n"
    "Host: 127.0.0.1\r\n"
    "Upgrade: websocket\r\n"
    "Connection: Upgrade\r\n"
    "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
    "Sec-WebSocket-Version: 13\r\n"
    "Sec-WebSocket-Protocol: chat\r\n"
    "\r\n";

static const uint8_t MASKED_HELLO[] = {
    0x81, 0x85, 0x37, 0xfa, 0x21, 0x3d, 0x5f, 0x9f, 0x4d, 0x51, 0x58,
};

static const uint8_t PLAIN_WORLD[] = {
    0x81, 0x05, 0x77, 0x6f, 0x72, 0x6c, 0x64,
};

static void fill_config(vyl_managed_static_config* config) {
    memset(config, 0, sizeof(*config));
    config->abi_version = VYL_MANAGED_STATIC_ABI_VERSION;
    config->struct_size = sizeof(*config);
    config->request_head_timeout_ns = 5000000000u;
    config->body_progress_timeout_ns = 5000000000u;
    config->response_write_timeout_ns = 5000000000u;
    config->keep_alive_idle_timeout_ns = 30000000000u;
    config->shutdown_grace_ns = 5000000000u;
    config->drain_idle_linger_ns = 200000000u;
    config->max_accepts_per_turn = 8;
    config->max_events_per_turn = 64;
    config->byte_budget_per_turn = 65536;
    config->poll_wait_ms = 10;
}

static int connect_port(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    assert(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
    assert(connect(fd, (struct sockaddr*)&address, sizeof(address)) == 0);

    struct timeval timeout = {5, 0};
    assert(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
    assert(setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) == 0);
    return fd;
}

static vyl_managed_static_event poll_kind(
    vyl_managed_static_server* server,
    uint32_t kind
) {
    vyl_managed_static_event events[8];
    uint8_t methods[256];
    uint8_t paths[4096];
    vyl_managed_static_poll_report report;
    for (;;) {
        memset(&report, 0, sizeof(report));
        assert(
            vyl_managed_static_poll_events(
                server,
                5000000000ull,
                events,
                8,
                methods,
                sizeof(methods),
                paths,
                sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t index = 0; index < report.copied; index++) {
            if (events[index].kind == kind) {
                return events[index];
            }
        }
    }
}

static void read_head(int fd) {
    char response[256] = {0};
    size_t total = 0;
    while (total < sizeof(response) - 1) {
        ssize_t received = recv(fd, response + total, 1, 0);
        assert(received == 1);
        total += 1;
        if (total >= 4 && memcmp(response + total - 4, "\r\n\r\n", 4) == 0) {
            break;
        }
    }
    assert(total >= 4);
    assert(memcmp(response, "HTTP/1.1 101 Switching Protocols\r\n", 34) == 0);
    assert(strstr(response, "Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=\r\n") != NULL);
    assert(strstr(response, "Sec-WebSocket-Protocol: chat\r\n") != NULL);
}

static void read_exact(int fd, uint8_t* buffer, size_t length) {
    size_t total = 0;
    while (total < length) {
        ssize_t received = recv(fd, buffer + total, length - total, 0);
        assert(received > 0);
        total += (size_t)received;
    }
}

int main(void) {
    vyl_managed_static_config config;
    fill_config(&config);
    static const uint8_t address[] = "127.0.0.1";
    config.address.data = address;
    config.address.length = sizeof(address) - 1;
    config.max_connections = 1;
    config.worker_count = 1;

    vyl_managed_static_server* server = NULL;
    assert(vyl_managed_static_new(&config, &server) == VYL_HTTP1_OK);
    assert(server != NULL);

    static const uint8_t method[] = "GET";
    static const uint8_t path[] = "/ws";
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){ method, sizeof(method) - 1 },
            (vyl_http1_bytes){ path, sizeof(path) - 1 },
            1024
        ) == VYL_HTTP1_OK
    );
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);

    uint16_t port = 0;
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);

    int fd = connect_port(port);
    assert(send(fd, HANDSHAKE, sizeof(HANDSHAKE) - 1, 0) == (ssize_t)(sizeof(HANDSHAKE) - 1));

    vyl_managed_static_event request = poll_kind(server, VYL_MANAGED_STATIC_EVENT_REQUEST);
    static const uint8_t protocol[] = "chat";
    assert(
        vyl_managed_static_host_upgrade_websocket_with_protocol(
            server,
            request.token,
            (vyl_http1_bytes){ protocol, sizeof(protocol) - 1 }
        ) == VYL_HTTP1_OK
    );
    read_head(fd);

    assert(
        send(fd, MASKED_HELLO, sizeof(MASKED_HELLO), 0) ==
        (ssize_t)sizeof(MASKED_HELLO)
    );
    poll_kind(server, VYL_MANAGED_STATIC_EVENT_TUNNEL_DATA);

    uint8_t inbound[256];
    size_t read = 0;
    assert(
        vyl_managed_static_host_tunnel_read(
            server,
            request.token,
            inbound,
            sizeof(inbound),
            &read
        ) == VYL_HTTP1_OK
    );
    assert(read == sizeof(MASKED_HELLO));
    assert(memcmp(inbound, MASKED_HELLO, sizeof(MASKED_HELLO)) == 0);

    size_t accepted = 0;
    assert(
        vyl_managed_static_host_tunnel_write(
            server,
            request.token,
            (vyl_http1_bytes){ PLAIN_WORLD, sizeof(PLAIN_WORLD) },
            &accepted
        ) == VYL_HTTP1_OK
    );
    assert(accepted == sizeof(PLAIN_WORLD));

    uint8_t echoed[sizeof(PLAIN_WORLD)];
    read_exact(fd, echoed, sizeof(echoed));
    assert(memcmp(echoed, PLAIN_WORLD, sizeof(PLAIN_WORLD)) == 0);

    assert(vyl_managed_static_host_tunnel_close(server, request.token) == VYL_HTTP1_OK);
    poll_kind(server, VYL_MANAGED_STATIC_EVENT_TUNNEL_CLOSED);

    char tail[64];
    for (;;) {
        ssize_t received = recv(fd, tail, sizeof(tail), 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
    }
    close(fd);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    vyl_managed_static_close_counts counts = {0};
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == 1);
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    return 0;
}
