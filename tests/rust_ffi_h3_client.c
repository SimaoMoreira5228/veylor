#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

static void expect_get(vyl_h3_client* client, const char* url) {
    vyl_h3_response* response = NULL;
    vyl_http1_result result = vyl_h3_client_request(
        client,
        (vyl_http1_bytes){ (const uint8_t*)"GET", 3 },
        (vyl_http1_bytes){ (const uint8_t*)url, strlen(url) },
        (vyl_http1_bytes){ NULL, 0 },
        &response
    );
    assert(result == VYL_HTTP1_OK);
    assert(response != NULL);
    assert(vyl_h3_response_status(response) == 200);
    assert(vyl_h3_response_header_count(response) > 0);
    vyl_http1_bytes body = { 0 };
    assert(vyl_h3_response_body(response, &body) == VYL_HTTP1_OK);
    assert(body.length == 6);
    assert(memcmp(body.data, "hello\n", 6) == 0);
    assert(vyl_h3_response_destroy(response) == VYL_HTTP1_OK);
}

int main(void) {
    vyl_managed_static_config config;
    fill_config(&config);
    static const uint8_t address[] = "127.0.0.1";
    config.address.data = address;
    config.address.length = sizeof(address) - 1;
    config.max_connections = 8;
    config.worker_count = 1;

    vyl_managed_static_server* server = NULL;
    assert(vyl_managed_static_new(&config, &server) == VYL_HTTP1_OK);

    static const uint8_t method[] = "GET";
    static const uint8_t path[] = "/hello";
    static const uint8_t response_body[] = "hello\n";
    assert(
        vyl_managed_static_register_static_route(
            server,
            (vyl_http1_bytes){ method, sizeof(method) - 1 },
            (vyl_http1_bytes){ path, sizeof(path) - 1 },
            200,
            (vyl_http1_bytes){ (const uint8_t*)"OK", 2 },
            (vyl_http1_bytes){ response_body, sizeof(response_body) - 1 }
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
    assert(client != NULL);

    char url[64];
    snprintf(url, sizeof(url), "https://localhost:%u/hello", (unsigned)h3_port);
    expect_get(client, url);
    expect_get(client, url);

    assert(vyl_h3_client_destroy(client) == VYL_HTTP1_OK);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    vyl_managed_static_close_counts counts = { 0 };
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    free(cert);
    free(key);
    return 0;
}
