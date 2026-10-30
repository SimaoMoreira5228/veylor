#include <arpa/inet.h>
#include <assert.h>
#include <netinet/in.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "veylor_http1.h"

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

static int exchange(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);

    struct sockaddr_in address = {0};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    assert(inet_pton(AF_INET, "127.0.0.1", &address.sin_addr) == 1);
    assert(connect(fd, (struct sockaddr*)&address, sizeof(address)) == 0);

    static const char request[] = "GET /hello HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    assert(send(fd, request, sizeof(request) - 1, 0) == (ssize_t)(sizeof(request) - 1));

    char response[1024] = {0};
    size_t total = 0;
    for (;;) {
        ssize_t received = recv(fd, response + total, sizeof(response) - total - 1, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < sizeof(response) - 1);
    }
    close(fd);

    response[total] = '\0';
    int matches = strstr(response, "HTTP/1.1 200 OK\r\n") == response
        && strstr(response, "\r\n\r\nhello") != NULL;
    return matches;
}

int main(void) {
    vyl_managed_static_config config;
    fill_config(&config);
    static const uint8_t address[] = "0.0.0.0";
    config.address.data = address;
    config.address.length = sizeof(address) - 1;
    config.max_connections = 1;
    config.worker_count = 1;

    vyl_managed_static_server* server = NULL;
    assert(vyl_managed_static_new(&config, &server) == VYL_HTTP1_OK);
    assert(server != NULL);

    static const uint8_t method[] = "GET";
    static const uint8_t path[] = "/hello";
    static const uint8_t body[] = "hello";
    assert(
        vyl_managed_static_register_static_route(
            server,
            (vyl_http1_bytes){ method, sizeof(method) - 1 },
            (vyl_http1_bytes){ path, sizeof(path) - 1 },
            200,
            (vyl_http1_bytes){ (const uint8_t*)"OK", 2 },
            (vyl_http1_bytes){ body, sizeof(body) - 1 }
        ) == VYL_HTTP1_OK
    );
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);

    uint16_t port = 0;
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);
    assert(exchange(port));

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    vyl_managed_static_close_counts counts = {0};
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);

    vyl_managed_static_config bad;
    fill_config(&bad);
    static const uint8_t bad_address[] = "999.999.999.999";
    bad.address.data = bad_address;
    bad.address.length = sizeof(bad_address) - 1;
    vyl_managed_static_server* rejected = NULL;
    assert(vyl_managed_static_new(&bad, &rejected) == VYL_HTTP1_INVALID_ARGUMENT);
    assert(rejected == NULL);
    return 0;
}
