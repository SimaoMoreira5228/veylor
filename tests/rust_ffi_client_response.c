#include "veylor_http1.h"

#include <assert.h>
#include <string.h>

static void test_client_response_reads(void) {
    static const uint8_t informational_exchange[] = "HTTP/1.1 100 Continue\r\n\r\n"
                                                    "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n";
    static const uint8_t fixed_response[] =
        "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789";
    static const uint8_t chunked_response[] =
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n"
        "3\r\nabc\r\n0\r\nDigest: sha-256=abc\r\n\r\n";
    static const uint8_t upgrade[] = "websocket";
    static const uint8_t upgrade_response[] =
        "HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\n"
        "Upgrade: websocket\r\n\r\ntunnel";
    vyl_client_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_header trailers[4] = {0};
    vyl_http1_client_read_event event = {0};
    size_t headers_required = 0;
    size_t trailers_required = 0;

    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_HEAD, NULL, 0, &connection) ==
        VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            informational_exchange,
            SIZE_MAX,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_INVALID_ARGUMENT
    );

    const size_t partial_length = sizeof("HTTP/1.1 100 Continue\r\n") - 1;
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            informational_exchange,
            partial_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_NEED_MORE
    );
    assert(event.kind == VYL_HTTP1_CLIENT_NEED_MORE);

    size_t input_offset = 0;
    size_t input_length = sizeof(informational_exchange) - 1;
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            informational_exchange,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_INFORMATIONAL);
    assert(event.response.status == 100);
    input_offset += event.consumed;
    input_length -= event.consumed;

    assert(
        vyl_http1_client_connection_read_response(
            connection,
            informational_exchange + input_offset,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_HEAD);
    assert(event.response.status == 200);
    assert(event.response.body_kind == VYL_HTTP1_RESPONSE_BODY_NONE);
    input_offset += event.consumed;
    input_length -= event.consumed;

    assert(
        vyl_http1_client_connection_read_response(
            connection,
            informational_exchange + input_offset,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_COMPLETE);
    assert(event.can_reuse == 1);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);

    connection = NULL;
    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_OTHER, NULL, 0, &connection) ==
        VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            fixed_response,
            sizeof(fixed_response) - 1,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_HEAD);
    assert(event.response.body_kind == VYL_HTTP1_RESPONSE_BODY_FIXED);
    assert(event.response.body_length == 10);
    input_offset = event.consumed;
    input_length = sizeof(fixed_response) - 1 - input_offset;
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            fixed_response + input_offset,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_BODY_DATA);
    assert(event.body_data.length == 10);
    assert(memcmp(fixed_response + input_offset + event.body_data.offset, "0123456789", 10) == 0);
    input_offset += event.consumed;
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            fixed_response + input_offset,
            0,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_COMPLETE);
    assert(event.can_reuse == 1);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);

    connection = NULL;
    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_OTHER, NULL, 0, &connection) ==
        VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            chunked_response,
            sizeof(chunked_response) - 1,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_HEAD);
    assert(event.response.body_kind == VYL_HTTP1_RESPONSE_BODY_CHUNKED);
    input_offset = event.consumed;
    input_length = sizeof(chunked_response) - 1 - input_offset;

    assert(
        vyl_http1_client_connection_read_response(
            connection,
            chunked_response + input_offset,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_BODY_DATA);
    assert(event.body_data.offset == 3);
    assert(event.body_data.length == 3);
    assert(memcmp(chunked_response + input_offset + event.body_data.offset, "abc", 3) == 0);
    input_offset += event.consumed;
    input_length -= event.consumed;

    assert(
        vyl_http1_client_connection_read_response(
            connection,
            chunked_response + input_offset,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_NEED_MORE
    );
    input_offset += event.consumed;
    input_length -= event.consumed;

    assert(
        vyl_http1_client_connection_read_response(
            connection,
            chunked_response + input_offset,
            input_length,
            0,
            headers,
            4,
            NULL,
            0,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(trailers_required == 1);
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            chunked_response + input_offset,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_NEED_MORE
    );
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            chunked_response + input_offset,
            input_length,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_TRAILERS);
    assert(event.trailer_count == 1);
    assert(
        memcmp(
            chunked_response + input_offset + trailers[0].name.offset,
            "Digest",
            trailers[0].name.length
        ) == 0
    );
    input_offset += event.consumed;
    input_length -= event.consumed;
    assert(input_length == 0);
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            chunked_response + sizeof(chunked_response) - 1,
            0,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_COMPLETE);
    assert(event.can_reuse == 1);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);

    connection = NULL;
    assert(
        vyl_http1_client_connection_new(
            VYL_HTTP1_REQUEST_OTHER,
            upgrade,
            sizeof(upgrade) - 1,
            &connection
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            upgrade_response,
            sizeof(upgrade_response) - 1,
            0,
            headers,
            1,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(headers_required == 2);
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            upgrade_response,
            sizeof(upgrade_response) - 1,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_UPGRADE);
    assert(event.response.status == 101);
    input_offset = event.consumed;
    assert(memcmp(upgrade_response + input_offset, "tunnel", 6) == 0);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_client_cancel_is_terminal(void) {
    static const uint8_t partial_response[] = "HTTP/1.1 200 OK\r\nContent-Len";
    static const uint8_t method[] = "POST";
    static const uint8_t target[] = "/upload";
    static const uint8_t host_name[] = "Host";
    static const uint8_t host_value[] = "example.test";
    vyl_client_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_header trailers[4] = {0};
    vyl_http1_client_read_event event = {0};
    size_t headers_required = 0;
    size_t trailers_required = 0;
    vyl_http1_field fields[] = {{
        .name = {.data = host_name, .length = sizeof(host_name) - 1},
        .value = {.data = host_value, .length = sizeof(host_value) - 1},
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
        vyl_http1_client_connection_read_response(
            connection,
            partial_response,
            sizeof(partial_response) - 1,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_NEED_MORE
    );
    assert(vyl_http1_client_connection_cancel(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            partial_response,
            sizeof(partial_response) - 1,
            0,
            headers,
            4,
            trailers,
            4,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_CANCELLED
    );
    assert(vyl_http1_client_connection_reset(connection) == VYL_HTTP1_CANCELLED);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);

    connection = NULL;
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
    assert(vyl_http1_client_connection_cancel(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_client_connection_advance_request_write(connection, written, &pending) ==
        VYL_HTTP1_CANCELLED
    );
    assert(vyl_http1_client_connection_reset(connection) == VYL_HTTP1_CANCELLED);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_client_cancel_mid_body_prevents_reuse(void) {
    static const uint8_t response[] = "HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n";
    static const uint8_t first_body_part[] = "ab";
    static const uint8_t unread_body_part[] = "cd";
    vyl_client_connection* connection = NULL;
    vyl_http1_header headers[2] = {0};
    vyl_http1_header trailers[2] = {0};
    vyl_http1_client_read_event event = {0};
    size_t headers_required = 0;
    size_t trailers_required = 0;

    assert(
        vyl_http1_client_connection_new(VYL_HTTP1_REQUEST_OTHER, NULL, 0, &connection) ==
        VYL_HTTP1_OK
    );
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            response,
            sizeof(response) - 1,
            0,
            headers,
            2,
            trailers,
            2,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_HEAD);
    assert(event.can_reuse == 0);
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            first_body_part,
            sizeof(first_body_part) - 1,
            0,
            headers,
            2,
            trailers,
            2,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_CLIENT_BODY_DATA);
    assert(event.can_reuse == 0);
    assert(vyl_http1_client_connection_cancel(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_client_connection_read_response(
            connection,
            unread_body_part,
            sizeof(unread_body_part) - 1,
            0,
            headers,
            2,
            trailers,
            2,
            &event,
            &headers_required,
            &trailers_required
        ) == VYL_HTTP1_CANCELLED
    );
    assert(vyl_http1_client_connection_reset(connection) == VYL_HTTP1_CANCELLED);
    assert(vyl_http1_client_connection_destroy(connection) == VYL_HTTP1_OK);
}

int main(void) {
    test_client_response_reads();
    test_client_cancel_is_terminal();
    test_client_cancel_mid_body_prevents_reuse();
    return 0;
}
