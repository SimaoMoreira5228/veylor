#include "veylor_http1.h"

#include <assert.h>
#include <string.h>

_Static_assert(sizeof(vyl_http1_span) == sizeof(size_t) * 2, "span layout must match Rust");
_Static_assert(
    sizeof(vyl_http1_header) == sizeof(vyl_http1_span) * 2,
    "header layout must match Rust"
);

static vyl_http1_field host_field(void) {
    static const uint8_t name[] = "Host";
    static const uint8_t value[] = "example.test";
    return (vyl_http1_field){
        .name = {.data = name, .length = sizeof(name) - 1},
        .value = {.data = value, .length = sizeof(value) - 1},
    };
}

static void test_fixed_request_body(void) {
    static const uint8_t method[] = "POST";
    static const uint8_t target[] = "/upload";
    static const uint8_t body[] = "hello";
    static const uint8_t too_long_body[] = "helloo";
    vyl_client_connection* connection = NULL;
    vyl_http1_field fields[] = {host_field()};
    uint8_t output[128];
    size_t written = 0;
    size_t required = 0;
    size_t pending = 0;
    size_t head_length = 0;

    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_OTHER, NULL, 0, &connection) ==
        VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_encode_request_head(
            connection,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = target, .length = sizeof(target) - 1},
            fields,
            1,
            VYL_HTTP1_REQUEST_BODY_FIXED,
            5,
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == required);
    head_length = written;
    assert(
        vyl_http1_client_connection_advance_request_write(connection, head_length + 1, &pending) ==
        VYL_HTTP1_WRITE_OVERRUN
    );
    assert(
        vyl_http1_client_connection_encode_request_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_WOULD_BLOCK
    );

    assert(
        vyl_http1_client_connection_advance_request_write(connection, head_length, &pending) ==
        VYL_HTTP1_OK
    );
    assert(pending == 0);
    assert(
        vyl_http1_client_connection_encode_request_body(
            connection,
            (vyl_http1_bytes){.data = too_long_body, .length = sizeof(too_long_body) - 1},
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_BODY_TOO_LONG
    );
    assert(
        vyl_http1_client_connection_encode_request_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            2,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == 2);
    assert(memcmp(output, "he", 2) == 0);
    assert(
        vyl_http1_client_connection_advance_request_write(connection, 1, &pending) == VYL_HTTP1_OK
    );
    assert(pending == 1);
    assert(
        vyl_http1_client_connection_encode_request_body(
            connection,
            (vyl_http1_bytes){.data = body + 2, .length = sizeof(body) - 3},
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_WOULD_BLOCK
    );
    assert(
        vyl_http1_client_connection_advance_request_write(connection, 1, &pending) == VYL_HTTP1_OK
    );
    assert(pending == 0);
    assert(
        vyl_http1_client_connection_encode_request_body(
            connection,
            (vyl_http1_bytes){.data = body + 2, .length = sizeof(body) - 3},
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == 3);
    assert(memcmp(output, "llo", 3) == 0);
    assert(
        vyl_http1_client_connection_advance_request_write(connection, written, &pending) ==
        VYL_HTTP1_OK
    );
    assert(pending == 0);
    assert(
        vyl_http1_client_connection_finish_request_body(
            connection,
            NULL,
            0,
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == 0);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_chunked_request_body(void) {
    static const uint8_t method[] = "POST";
    static const uint8_t target[] = "/stream";
    static const uint8_t body[] = "abc";
    static const uint8_t chunk[] = "3\r\nabc\r\n";
    static const uint8_t trailer_name[] = "Digest";
    static const uint8_t trailer_value[] = "sha-256=abc";
    static const uint8_t end[] = "0\r\nDigest: sha-256=abc\r\n\r\n";
    vyl_client_connection* connection = NULL;
    vyl_http1_field fields[] = {host_field()};
    vyl_http1_field trailers[] = {{
        .name = {.data = trailer_name, .length = sizeof(trailer_name) - 1},
        .value = {.data = trailer_value, .length = sizeof(trailer_value) - 1},
    }};
    uint8_t output[128];
    size_t written = 0;
    size_t required = 0;
    size_t pending = 0;

    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_OTHER, NULL, 0, &connection) ==
        VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_encode_request_head(
            connection,
            (vyl_http1_bytes){.data = method, .length = sizeof(method) - 1},
            (vyl_http1_bytes){.data = target, .length = sizeof(target) - 1},
            fields,
            1,
            VYL_HTTP1_REQUEST_BODY_CHUNKED,
            0,
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_advance_request_write(connection, written, &pending) ==
        VYL_HTTP1_OK
    );
    assert(pending == 0);

    memset(output, 0xaa, sizeof(output));
    assert(
        vyl_http1_client_connection_encode_request_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(chunk) - 2,
            &written,
            &required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(required == sizeof(chunk) - 1);
    assert(output[0] == 0xaa);
    assert(
        vyl_http1_client_connection_encode_request_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == sizeof(chunk) - 1);
    assert(memcmp(output, chunk, written) == 0);
    assert(
        vyl_http1_client_connection_advance_request_write(connection, written, &pending) ==
        VYL_HTTP1_OK
    );
    assert(pending == 0);

    assert(
        vyl_http1_client_connection_finish_request_body(
            connection,
            trailers,
            1,
            output,
            sizeof(end) - 2,
            &written,
            &required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(required == sizeof(end) - 1);
    assert(output[0] == '3');
    assert(
        vyl_http1_client_connection_finish_request_body(
            connection,
            trailers,
            1,
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == sizeof(end) - 1);
    assert(memcmp(output, end, written) == 0);
    assert(
        vyl_http1_client_connection_advance_request_write(connection, written, &pending) ==
        VYL_HTTP1_OK
    );
    assert(pending == 0);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);
}

int main(void) {
    static const uint8_t request[] =
        "GET /health HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\n\r\n";
    static const uint8_t body_request[] =
        "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n"
        "Expect: 100-continue\r\nConnection: close\r\n\r\n";
    static const uint8_t upgrade[] = "websocket";
    static const uint8_t request_method[] = "HEAD";
    static const uint8_t request_target[] = "/health";
    static const uint8_t wrong_method[] = "POST";
    static const uint8_t upload_method[] = "POST";
    static const uint8_t upload_target[] = "/upload";
    static const uint8_t host_name[] = "Host";
    static const uint8_t host_value[] = "example.test";
    static const uint8_t response_reason[] = "OK";
    static const uint8_t response_type_name[] = "Content-Type";
    static const uint8_t response_type_value[] = "text/plain";
    static const uint8_t expected_head[] = "HEAD /health HTTP/1.1\r\nHost: example.test\r\n\r\n";
    static const uint8_t expected_response[] =
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 5\r\n\r\n";
    static const uint8_t expected_closed_response[] =
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nConnection: close\r\n"
        "Content-Length: 5\r\n\r\n";
    static const uint8_t expected_upload_head[] =
        "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n\r\n";
    static const size_t prefix_length =
        sizeof("GET /health HTTP/1.1\r\nHost: example.test\r\nAcc") - 1;
    vyl_server_connection* connection = NULL;
    vyl_client_connection* client = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    vyl_http1_field request_fields[] = {{
        .name = {.data = host_name, .length = sizeof(host_name) - 1},
        .value = {.data = host_value, .length = sizeof(host_value) - 1},
    }};
    vyl_http1_field response_fields[] = {{
        .name = {.data = response_type_name, .length = sizeof(response_type_name) - 1},
        .value = {.data = response_type_value, .length = sizeof(response_type_value) - 1},
    }};
    vyl_http1_field excessive_fields[VYL_HTTP1_MAX_REQUEST_FIELDS + 1] = {0};
    uint8_t request_output[128];
    uint8_t response_output[128];
    size_t request_written = SIZE_MAX;
    size_t request_required = SIZE_MAX;
    size_t headers_required = SIZE_MAX;
    size_t response_written = 0;
    size_t response_required = 0;
    uint32_t response_body_allowed = 0;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    assert(connection != NULL);
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            prefix_length,
            headers,
            2,
            &head,
            &headers_required
        ) == VYL_HTTP1_NEED_MORE
    );
    assert(headers_required == 0);

    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            2,
            &head,
            &headers_required
        ) == VYL_HTTP1_OK
    );
    assert(head.target_form == VYL_HTTP1_TARGET_ORIGIN);
    assert(head.header_count == 2);
    assert(headers_required == 2);
    assert(head.method.length == 3);
    assert(memcmp(request + head.method.offset, "GET", 3) == 0);
    assert(memcmp(request + head.target.offset, "/health", 7) == 0);

    memset(response_output, 0xaa, sizeof(response_output));
    assert(
        vyl_http1_server_connection_encode_response_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            2,
            200,
            (vyl_http1_bytes){.data = response_reason, .length = sizeof(response_reason) - 1},
            response_fields,
            1,
            VYL_HTTP1_RESPONSE_FRAMING_FIXED,
            5,
            1,
            0,
            response_output,
            8,
            &response_written,
            &response_body_allowed,
            &response_required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(response_written == 0);
    assert(response_body_allowed == 0);
    assert(response_required == sizeof(expected_response) - 1);
    assert(response_output[0] == 0xaa);
    assert(
        vyl_http1_server_connection_encode_response_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            2,
            200,
            (vyl_http1_bytes){.data = response_reason, .length = sizeof(response_reason) - 1},
            response_fields,
            1,
            VYL_HTTP1_RESPONSE_FRAMING_FIXED,
            5,
            1,
            0,
            response_output,
            sizeof(response_output),
            &response_written,
            &response_body_allowed,
            &response_required
        ) == VYL_HTTP1_OK
    );
    assert(response_written == sizeof(expected_response) - 1);
    assert(response_required == response_written);
    assert(response_body_allowed == 1);
    assert(memcmp(response_output, expected_response, response_written) == 0);
    assert(
        vyl_http1_server_connection_encode_response_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            2,
            200,
            (vyl_http1_bytes){.data = response_reason, .length = sizeof(response_reason) - 1},
            response_fields,
            1,
            VYL_HTTP1_RESPONSE_FRAMING_FIXED,
            5,
            1,
            0,
            response_output,
            sizeof(response_output),
            &response_written,
            &response_body_allowed,
            &response_required
        ) == VYL_HTTP1_BAD_STATE
    );

    assert(vyl_http1_server_connection_reset(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            1,
            &head,
            &headers_required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(headers_required == 2);
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            2,
            &head,
            &headers_required
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_http1_server_connection_encode_response_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            2,
            200,
            (vyl_http1_bytes){.data = response_reason, .length = sizeof(response_reason) - 1},
            response_fields,
            1,
            VYL_HTTP1_RESPONSE_FRAMING_FIXED,
            5,
            0,
            0,
            response_output,
            sizeof(response_output),
            &response_written,
            &response_body_allowed,
            &response_required
        ) == VYL_HTTP1_OK
    );
    assert(response_written == sizeof(expected_closed_response) - 1);
    assert(memcmp(response_output, expected_closed_response, response_written) == 0);

    assert(vyl_http1_server_connection_reset(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            body_request,
            sizeof(body_request) - 1,
            headers,
            4,
            &head,
            &headers_required
        ) == VYL_HTTP1_OK
    );
    assert(head.content_length == 5);
    assert(
        head.flags ==
        (VYL_HTTP1_HAS_CONTENT_LENGTH | VYL_HTTP1_EXPECT_CONTINUE | VYL_HTTP1_CONNECTION_CLOSE)
    );
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            SIZE_MAX,
            headers,
            4,
            &head,
            &headers_required
        ) == VYL_HTTP1_INVALID_ARGUMENT
    );

    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);

    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_HEAD, NULL, 0, &client) == VYL_HTTP1_OK
    );
    memset(request_output, 0xaa, sizeof(request_output));
    assert(
        vyl_http1_client_connection_encode_request_head(
            client,
            (vyl_http1_bytes){request_method, sizeof(request_method) - 1},
            (vyl_http1_bytes){request_target, sizeof(request_target) - 1},
            request_fields,
            1,
            VYL_HTTP1_REQUEST_BODY_NONE,
            0,
            request_output,
            8,
            &request_written,
            &request_required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(request_written == 0);
    assert(request_required == sizeof(expected_head) - 1);
    assert(request_output[0] == 0xaa);
    assert(
        vyl_http1_client_connection_encode_request_head(
            client,
            (vyl_http1_bytes){wrong_method, sizeof(wrong_method) - 1},
            (vyl_http1_bytes){request_target, sizeof(request_target) - 1},
            request_fields,
            1,
            VYL_HTTP1_REQUEST_BODY_FIXED,
            5,
            request_output,
            sizeof(request_output),
            &request_written,
            &request_required
        ) == VYL_HTTP1_INVALID_ARGUMENT
    );
    assert(
        vyl_http1_client_connection_encode_request_head(
            client,
            (vyl_http1_bytes){request_method, sizeof(request_method) - 1},
            (vyl_http1_bytes){request_target, sizeof(request_target) - 1},
            request_fields,
            1,
            VYL_HTTP1_REQUEST_BODY_NONE,
            0,
            request_output,
            sizeof(request_output),
            &request_written,
            &request_required
        ) == VYL_HTTP1_OK
    );
    assert(request_written == sizeof(expected_head) - 1);
    assert(request_required == request_written);
    assert(memcmp(request_output, expected_head, request_written) == 0);
    assert(
        vyl_http1_client_connection_encode_request_head(
            client,
            (vyl_http1_bytes){request_method, sizeof(request_method) - 1},
            (vyl_http1_bytes){request_target, sizeof(request_target) - 1},
            excessive_fields,
            VYL_HTTP1_MAX_REQUEST_FIELDS + 1,
            VYL_HTTP1_REQUEST_BODY_NONE,
            0,
            request_output,
            sizeof(request_output),
            &request_written,
            &request_required
        ) == VYL_HTTP1_CAPACITY
    );
    assert(request_written == 0);
    assert(request_required == 0);

    assert(
        vyl_http1_client_connection_encode_request_head(
            client,
            (vyl_http1_bytes){request_method, sizeof(request_method) - 1},
            (vyl_http1_bytes){request_target, sizeof(request_target) - 1},
            request_fields,
            1,
            VYL_HTTP1_REQUEST_BODY_FIXED,
            5,
            request_output,
            sizeof(request_output),
            &request_written,
            &request_required
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(request_written == 0);
    assert(vyl_http1_client_connection_destroy(client) == VYL_HTTP1_OK);

    client = NULL;
    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_OTHER, NULL, 0, &client) == VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_encode_request_head(
            client,
            (vyl_http1_bytes){upload_method, sizeof(upload_method) - 1},
            (vyl_http1_bytes){upload_target, sizeof(upload_target) - 1},
            request_fields,
            1,
            VYL_HTTP1_REQUEST_BODY_FIXED,
            5,
            request_output,
            sizeof(request_output),
            &request_written,
            &request_required
        ) == VYL_HTTP1_OK
    );
    assert(request_written == sizeof(expected_upload_head) - 1);
    assert(memcmp(request_output, expected_upload_head, request_written) == 0);
    assert(vyl_http1_client_connection_destroy(client) == VYL_HTTP1_OK);

    client = NULL;
    assert(
        vyl_http1_client_connection_new(
            VYL_HTTP1_REQUEST_OTHER,
            upgrade,
            sizeof(upgrade) - 1,
            &client
        ) == VYL_HTTP1_OK
    );
    assert(vyl_http1_client_connection_destroy(client) == VYL_HTTP1_OK);

    client = NULL;
    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_OTHER, upgrade, SIZE_MAX, &client) ==
        VYL_HTTP1_INVALID_ARGUMENT
    );
    assert(client == NULL);

    assert(vyl_http1_client_connection_new(99, NULL, 0, &client) == VYL_HTTP1_INVALID_ARGUMENT);
    assert(client == NULL);
    test_fixed_request_body();
    test_chunked_request_body();
    return 0;
}
