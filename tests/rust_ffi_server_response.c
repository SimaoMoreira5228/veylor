#include "veylor_http1.h"

#include <assert.h>
#include <string.h>

/* NOTE: cross-boundary layout pins (all declared targets are 64-bit, so
   size_t is 8 bytes): the C declarations must stay byte-identical to the Rust
   #[repr(C)] originals. Any field addition, removal, or reorder breaks these. */
_Static_assert(
    sizeof(vyl_http1_request_head) == 104,
    "request head layout must match Rust RequestHead"
);
static void parse_request(
    vyl_server_connection* connection,
    const uint8_t* request,
    size_t request_length,
    vyl_http1_header* headers,
    vyl_http1_request_head* head
) {
    size_t required = 0;
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            request_length,
            headers,
            4,
            head,
            &required
        ) == VYL_HTTP1_OK
    );
}

static size_t encode_response_head(
    vyl_server_connection* connection,
    const uint8_t* request,
    size_t request_length,
    vyl_http1_header* request_headers,
    uint16_t status,
    uint32_t framing,
    uint64_t content_length,
    uint8_t* output,
    size_t output_capacity,
    uint32_t* body_allowed
) {
    static const uint8_t reason[] = "OK";
    static const uint8_t upgrade_name[] = "Upgrade";
    static const uint8_t upgrade_value[] = "websocket";
    vyl_http1_field upgrade = {
        .name = {.data = upgrade_name, .length = sizeof(upgrade_name) - 1},
        .value = {.data = upgrade_value, .length = sizeof(upgrade_value) - 1},
    };
    const vyl_http1_field* fields = status == 101 ? &upgrade : NULL;
    size_t field_count = status == 101 ? 1 : 0;
    size_t written = 0;
    size_t required = 0;
    assert(
        vyl_http1_server_connection_encode_response_head(
            connection,
            request,
            request_length,
            request_headers,
            4,
            status,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            fields,
            field_count,
            framing,
            content_length,
            1,
            0,
            output,
            output_capacity,
            &written,
            body_allowed,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == required);
    return written;
}

static void advance_response(vyl_server_connection* connection, size_t bytes) {
    size_t pending = SIZE_MAX;
    assert(
        vyl_http1_server_connection_advance_response_write(connection, bytes, &pending) ==
        VYL_HTTP1_OK
    );
}

static void test_fixed_response_body_partial_writes(void) {
    static const uint8_t request[] =
        "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n\r\n";
    static const uint8_t body[] = "hello";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    uint8_t output[64];
    uint32_t body_allowed = 0;
    size_t consumed = 0;
    size_t written = 0;
    size_t required = 0;
    size_t head_length;
    size_t pending = 0;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, request, sizeof(request) - 1, headers, &head);
    head_length = encode_response_head(
        connection,
        request,
        sizeof(request) - 1,
        headers,
        200,
        VYL_HTTP1_RESPONSE_FRAMING_FIXED,
        5,
        output,
        sizeof(output),
        &body_allowed
    );
    assert(body_allowed == 1);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_WOULD_BLOCK
    );
    advance_response(connection, head_length - 1);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_WOULD_BLOCK
    );
    advance_response(connection, 1);

    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            2,
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(consumed == 2);
    assert(written == 2);
    assert(memcmp(output, "he", 2) == 0);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body + consumed, .length = sizeof(body) - 1 - consumed},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_WOULD_BLOCK
    );
    assert(
        vyl_http1_server_connection_advance_response_write(connection, 1, &pending) == VYL_HTTP1_OK
    );
    assert(pending == 1);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body + 2, .length = sizeof(body) - 3},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_WOULD_BLOCK
    );
    assert(
        vyl_http1_server_connection_advance_response_write(connection, 1, &pending) == VYL_HTTP1_OK
    );
    assert(pending == 0);

    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body + 2, .length = sizeof(body) - 3},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(consumed == 3);
    assert(written == 3);
    assert(memcmp(output, "llo", 3) == 0);
    advance_response(connection, written);
    assert(
        vyl_http1_server_connection_finish_response_body(
            connection,
            NULL,
            0,
            NULL,
            0,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_chunked_response_body_and_trailers(void) {
    static const uint8_t request[] = "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    static const uint8_t body[] = "Wiki";
    static const uint8_t trailer_name[] = "Digest";
    static const uint8_t trailer_value[] = "ok";
    static const uint8_t expected_chunk[] = "4\r\nWiki\r\n";
    static const uint8_t expected_end[] = "0\r\nDigest: ok\r\n\r\n";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    vyl_http1_field trailer = {
        .name = {.data = trailer_name, .length = sizeof(trailer_name) - 1},
        .value = {.data = trailer_value, .length = sizeof(trailer_value) - 1},
    };
    uint8_t output[64];
    uint32_t body_allowed = 0;
    size_t consumed = 0;
    size_t written = 0;
    size_t required = 0;
    size_t head_length;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, request, sizeof(request) - 1, headers, &head);
    head_length = encode_response_head(
        connection,
        request,
        sizeof(request) - 1,
        headers,
        200,
        VYL_HTTP1_RESPONSE_FRAMING_CHUNKED,
        0,
        output,
        sizeof(output),
        &body_allowed
    );
    assert(body_allowed == 1);
    advance_response(connection, head_length);

    memset(output, 0xa5, sizeof(output));
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(expected_chunk) - 2,
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(required == sizeof(expected_chunk) - 1);
    assert(consumed == 0);
    assert(output[0] == 0xa5);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(consumed == sizeof(body) - 1);
    assert(written == sizeof(expected_chunk) - 1);
    assert(memcmp(output, expected_chunk, written) == 0);
    advance_response(connection, written);

    assert(
        vyl_http1_server_connection_finish_response_body(
            connection,
            &trailer,
            1,
            output,
            8,
            &written,
            &required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(required == sizeof(expected_end) - 1);
    assert(
        vyl_http1_server_connection_finish_response_body(
            connection,
            &trailer,
            1,
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(written == sizeof(expected_end) - 1);
    assert(memcmp(output, expected_end, written) == 0);
    advance_response(connection, written);
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_fixed_response_rejects_short_body(void) {
    static const uint8_t request[] = "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    static const uint8_t body[] = "a";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    uint8_t output[64];
    uint32_t body_allowed = 0;
    size_t consumed = 0;
    size_t written = 0;
    size_t required = 0;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, request, sizeof(request) - 1, headers, &head);
    size_t head_length = encode_response_head(
        connection,
        request,
        sizeof(request) - 1,
        headers,
        200,
        VYL_HTTP1_RESPONSE_FRAMING_FIXED,
        2,
        output,
        sizeof(output),
        &body_allowed
    );
    assert(body_allowed == 1);
    advance_response(connection, head_length);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(consumed == 1);
    advance_response(connection, written);
    assert(
        vyl_http1_server_connection_finish_response_body(
            connection,
            NULL,
            0,
            NULL,
            0,
            &written,
            &required
        ) == VYL_HTTP1_PROTOCOL_ERROR
    );
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_close_delimited_response_body(void) {
    static const uint8_t request[] = "GET /stream HTTP/1.1\r\nHost: example.test\r\n\r\n";
    static const uint8_t body[] = "abc";
    static const uint8_t expected_head[] = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\n";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    uint8_t output[64];
    uint32_t body_allowed = 0;
    size_t consumed = 0;
    size_t written = 0;
    size_t required = 0;
    size_t head_length;
    size_t pending = SIZE_MAX;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, request, sizeof(request) - 1, headers, &head);
    head_length = encode_response_head(
        connection,
        request,
        sizeof(request) - 1,
        headers,
        200,
        VYL_HTTP1_RESPONSE_FRAMING_CLOSE_DELIMITED,
        0,
        output,
        sizeof(output),
        &body_allowed
    );
    assert(body_allowed == 1);
    assert(head_length == sizeof(expected_head) - 1);
    assert(memcmp(output, expected_head, head_length) == 0);
    advance_response(connection, head_length);

    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            2,
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(consumed == 2);
    assert(written == 2);
    assert(memcmp(output, "ab", 2) == 0);
    assert(
        vyl_http1_server_connection_advance_response_write(connection, 1, &pending) == VYL_HTTP1_OK
    );
    assert(pending == 1);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body + consumed, .length = sizeof(body) - 1 - consumed},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_WOULD_BLOCK
    );
    assert(
        vyl_http1_server_connection_advance_response_write(connection, 1, &pending) == VYL_HTTP1_OK
    );
    assert(pending == 0);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body + 2, .length = sizeof(body) - 3},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(consumed == 1);
    assert(written == 1);
    assert(output[0] == 'c');
    advance_response(connection, written);
    assert(
        vyl_http1_server_connection_finish_response_body(
            connection,
            NULL,
            0,
            NULL,
            0,
            &written,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_head_response_rejects_body_output(void) {
    static const uint8_t request[] = "HEAD / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    static const uint8_t body[] = "not sent";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    uint8_t output[64];
    uint32_t body_allowed = 1;
    size_t consumed = 0;
    size_t written = 0;
    size_t required = 0;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, request, sizeof(request) - 1, headers, &head);
    size_t head_length = encode_response_head(
        connection,
        request,
        sizeof(request) - 1,
        headers,
        200,
        VYL_HTTP1_RESPONSE_FRAMING_FIXED,
        sizeof(body) - 1,
        output,
        sizeof(output),
        &body_allowed
    );
    assert(body_allowed == 0);
    advance_response(connection, head_length);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_no_content_and_upgrade_responses_reject_body_output(void) {
    static const uint8_t no_content_request[] =
        "POST /empty HTTP/1.1\r\nHost: example.test\r\n\r\n";
    static const uint8_t upgrade_request[] =
        "GET /socket HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\n"
        "Upgrade: websocket\r\n\r\n";
    static const uint8_t body[] = "not sent";
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    uint8_t output[128];
    uint32_t body_allowed = 1;
    size_t consumed = 0;
    size_t written = 0;
    size_t required = 0;
    size_t head_length;
    vyl_server_connection* connection = NULL;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, no_content_request, sizeof(no_content_request) - 1, headers, &head);
    head_length = encode_response_head(
        connection,
        no_content_request,
        sizeof(no_content_request) - 1,
        headers,
        204,
        VYL_HTTP1_RESPONSE_FRAMING_FIXED,
        0,
        output,
        sizeof(output),
        &body_allowed
    );
    assert(body_allowed == 0);
    advance_response(connection, head_length);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);

    connection = NULL;
    memset(headers, 0, sizeof(headers));
    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, upgrade_request, sizeof(upgrade_request) - 1, headers, &head);
    head_length = encode_response_head(
        connection,
        upgrade_request,
        sizeof(upgrade_request) - 1,
        headers,
        101,
        VYL_HTTP1_RESPONSE_FRAMING_FIXED,
        0,
        output,
        sizeof(output),
        &body_allowed
    );
    assert(body_allowed == 0);
    advance_response(connection, head_length);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

int main(void) {
    test_fixed_response_body_partial_writes();
    test_chunked_response_body_and_trailers();
    test_fixed_response_rejects_short_body();
    test_close_delimited_response_body();
    test_head_response_rejects_body_output();
    test_no_content_and_upgrade_responses_reject_body_output();
    return 0;
}
