#include <assert.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "veylor_http1.h"

static vyl_managed_static_server* server_handle;
static atomic_bool stop_dispatch;

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

static uint8_t* read_file(const char* path, size_t* length) {
    FILE* file = fopen(path, "rb");
    assert(file != NULL);
    assert(fseek(file, 0, SEEK_END) == 0);
    long size = ftell(file);
    assert(size > 0);
    assert(fseek(file, 0, SEEK_SET) == 0);
    uint8_t* bytes = malloc((size_t)size);
    assert(bytes != NULL);
    assert(fread(bytes, 1, (size_t)size, file) == (size_t)size);
    fclose(file);
    *length = (size_t)size;
    return bytes;
}

static void* dispatch(void* unused) {
    (void)unused;
    vyl_managed_static_event events[16];
    uint8_t methods[256];
    uint8_t paths[4096];
    while (!atomic_load(&stop_dispatch)) {
        vyl_managed_static_poll_report report;
        assert(
            vyl_managed_static_poll_events(
                server_handle,
                10000000,
                events,
                16,
                methods,
                sizeof(methods),
                paths,
                sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t index = 0; index < report.copied; index++) {
            vyl_managed_static_token token = events[index].token;
            if (events[index].kind == VYL_MANAGED_STATIC_EVENT_REQUEST) {
                assert(
                    vyl_managed_static_host_upgrade_websocket_with_protocol(
                        server_handle,
                        token,
                        (vyl_http1_bytes){ (const uint8_t*)"chat", 4 }
                    ) == VYL_HTTP1_OK
                );
                size_t accepted = 0;
                assert(
                    vyl_managed_static_host_tunnel_write(
                        server_handle,
                        token,
                        (vyl_http1_bytes){ (const uint8_t*)"welcome", 7 },
                        &accepted
                    ) == VYL_HTTP1_OK
                );
                assert(accepted == 7);
            } else if (events[index].kind == VYL_MANAGED_STATIC_EVENT_TUNNEL_DATA) {
                uint8_t buffer[128];
                size_t read = 0;
                assert(
                    vyl_managed_static_host_tunnel_read(
                        server_handle,
                        token,
                        buffer,
                        sizeof(buffer),
                        &read
                    ) == VYL_HTTP1_OK
                );
                size_t accepted = 0;
                assert(
                    vyl_managed_static_host_tunnel_write(
                        server_handle,
                        token,
                        (vyl_http1_bytes){ buffer, read },
                        &accepted
                    ) == VYL_HTTP1_OK
                );
                assert(accepted == read);
            } else if (events[index].kind == VYL_MANAGED_STATIC_EVENT_TUNNEL_CLOSED) {
                assert(vyl_managed_static_host_tunnel_close(server_handle, token) == VYL_HTTP1_OK);
            }
        }
    }
    return NULL;
}

int main(void) {
    atomic_init(&stop_dispatch, false);
    vyl_managed_static_config config;
    fill_config(&config);
    static const uint8_t address[] = "127.0.0.1";
    config.address.data = address;
    config.address.length = sizeof(address) - 1;
    config.max_connections = 8;
    config.worker_count = 1;

    vyl_managed_static_server* server = NULL;
    assert(vyl_managed_static_new(&config, &server) == VYL_HTTP1_OK);
    server_handle = server;

    static const uint8_t method[] = "CONNECT";
    static const uint8_t path[] = "/ws";
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){ method, sizeof(method) - 1 },
            (vyl_http1_bytes){ path, sizeof(path) - 1 },
            1024
        ) == VYL_HTTP1_OK
    );

    size_t cert_len = 0;
    size_t key_len = 0;
    uint8_t* cert = read_file("crates/veylor-tls/tests/testdata/leaf_cert.der", &cert_len);
    uint8_t* key = read_file("crates/veylor-tls/tests/testdata/leaf_key.der", &key_len);
    assert(vyl_managed_static_enable_tls(server, cert, cert_len, key, key_len, 8) == VYL_HTTP1_OK);
    assert(vyl_managed_static_enable_h3(server, 0) == VYL_HTTP1_OK);
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);

    uint16_t h3_port = 0;
    assert(vyl_managed_static_h3_addr(server, &h3_port) == VYL_HTTP1_OK);
    assert(h3_port != 0);

    pthread_t dispatch_thread;
    assert(pthread_create(&dispatch_thread, NULL, dispatch, NULL) == 0);

    vyl_h3_client_config client_config;
    memset(&client_config, 0, sizeof(client_config));
    client_config.abi_version = VYL_H3_CLIENT_ABI_VERSION;
    client_config.struct_size = sizeof(client_config);
    client_config.address = (vyl_http1_bytes){ address, sizeof(address) - 1 };
    client_config.server_name = (vyl_http1_bytes){ (const uint8_t*)"localhost", 9 };
    client_config.root_cert = (vyl_http1_bytes){ cert, cert_len };
    client_config.port = h3_port;
    client_config.idle_timeout_ns = 30000000000u;

    vyl_h3_client* client = NULL;
    assert(vyl_h3_client_new(&client_config, &client) == VYL_HTTP1_OK);

    char url[64];
    snprintf(url, sizeof(url), "https://localhost:%u/ws", (unsigned)h3_port);
    vyl_h3_tunnel* tunnel = NULL;
    assert(
        vyl_h3_client_connect_tunnel(
            client,
            (vyl_http1_bytes){ (const uint8_t*)url, strlen(url) },
            (vyl_http1_bytes){ (const uint8_t*)"websocket", 9 },
            (vyl_http1_bytes){ (const uint8_t*)"chat", 4 },
            &tunnel
        ) == VYL_HTTP1_OK
    );
    assert(tunnel != NULL);

    vyl_http1_bytes welcome = { 0 };
    assert(vyl_h3_tunnel_read(tunnel, &welcome) == VYL_HTTP1_OK);
    assert(welcome.length == 7);
    assert(memcmp(welcome.data, "welcome", 7) == 0);

    assert(
        vyl_h3_tunnel_write(
            tunnel,
            (vyl_http1_bytes){ (const uint8_t*)"ping", 4 }
        ) == VYL_HTTP1_OK
    );
    vyl_http1_bytes echoed = { 0 };
    assert(vyl_h3_tunnel_read(tunnel, &echoed) == VYL_HTTP1_OK);
    assert(echoed.length == 4);
    assert(memcmp(echoed.data, "ping", 4) == 0);

    assert(vyl_h3_tunnel_close(tunnel) == VYL_HTTP1_OK);
    assert(vyl_h3_tunnel_destroy(tunnel) == VYL_HTTP1_OK);
    assert(vyl_h3_client_destroy(client) == VYL_HTTP1_OK);

    atomic_store(&stop_dispatch, true);
    assert(pthread_join(dispatch_thread, NULL) == 0);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    vyl_managed_static_close_counts counts = { 0 };
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    free(cert);
    free(key);
    return 0;
}
