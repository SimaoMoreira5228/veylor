#include "veylor_http1.h"

#include <assert.h>
#include <string.h>

static void parse_request(
    vyl_server_connection* connection,
    const uint8_t* input,
    size_t input_length,
    vyl_http1_header* headers,
    vyl_http1_request_head* out_head
) {
    size_t required = 0;
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            input,
            input_length,
            headers,
            4,
            out_head,
            &required
        ) == VYL_HTTP1_OK
    );
}

static void test_cancel_during_request_body_is_terminal(void) {
    static const uint8_t request[] =
        "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 4\r\n\r\n";
    static const uint8_t first_body_part[] = "ab";
    static const uint8_t rest_body[] = "cd";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    vyl_http1_server_body_event event = {0};
    size_t headers_required = 0;
    size_t trailers_required = 0;
    uint32_t admission = UINT32_MAX;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, request, sizeof(request) - 1, headers, &head);
    assert(
        vyl_http1_server_connection_admit_body(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            NULL,
            0,
            NULL,
            0,
            1,
            &admission
        ) == VYL_HTTP1_OK
    );
    assert(admission == VYL_HTTP1_ADMISSION_READY);

    assert(
        vyl_http1_server_connection_read_body(
            connection,
            first_body_part,
            sizeof(first_body_part) - 1,
            0,
            NULL,
            0,
            &event,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_BODY_DATA);
    assert(event.consumed == sizeof(first_body_part) - 1);

    assert(vyl_http1_server_connection_cancel(connection) == VYL_HTTP1_OK);
    assert(vyl_http1_server_connection_cancel(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_server_connection_read_body(
            connection,
            rest_body,
            sizeof(rest_body) - 1,
            0,
            NULL,
            0,
            &event,
            &trailers_required
        ) == VYL_HTTP1_CANCELLED
    );
    assert(
        vyl_http1_server_connection_admit_body(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            NULL,
            0,
            NULL,
            0,
            1,
            &admission
        ) == VYL_HTTP1_CANCELLED
    );
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            4,
            &head,
            &headers_required
        ) == VYL_HTTP1_CANCELLED
    );
    assert(vyl_http1_server_connection_reset(connection) == VYL_HTTP1_CANCELLED);
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_cancel_during_response_write_is_terminal(void) {
    static const uint8_t request[] = "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n";
    static const uint8_t reason[] = "OK";
    static const uint8_t body[] = "data";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    uint8_t output[64];
    size_t written = 0;
    size_t required = 0;
    size_t pending = 0;
    size_t consumed = 0;
    uint32_t body_allowed = 0;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    parse_request(connection, request, sizeof(request) - 1, headers, &head);
    assert(
        vyl_http1_server_connection_encode_response_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            4,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            NULL,
            0,
            VYL_HTTP1_RESPONSE_FRAMING_FIXED,
            sizeof(body) - 1,
            1,
            0,
            output,
            sizeof(output),
            &written,
            &body_allowed,
            &required
        ) == VYL_HTTP1_OK
    );
    assert(body_allowed == 1);

    assert(vyl_http1_server_connection_cancel(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_server_connection_advance_response_write(connection, written, &pending) ==
        VYL_HTTP1_CANCELLED
    );
    assert(pending == 0);
    assert(
        vyl_http1_server_connection_encode_response_body(
            connection,
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1},
            output,
            sizeof(output),
            &consumed,
            &written,
            &required
        ) == VYL_HTTP1_CANCELLED
    );
    assert(consumed == 0);
    assert(written == 0);
    assert(
        vyl_http1_server_connection_finish_response_body(
            connection,
            NULL,
            0,
            output,
            sizeof(output),
            &written,
            &required
        ) == VYL_HTTP1_CANCELLED
    );
    assert(written == 0);
    assert(vyl_http1_server_connection_reset(connection) == VYL_HTTP1_CANCELLED);
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

int main(void) {
    test_cancel_during_request_body_is_terminal();
    test_cancel_during_response_write_is_terminal();
    return 0;
}
