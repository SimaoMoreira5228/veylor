#include "veylor_http1.h"

#include <assert.h>
#include <string.h>


/* NOTE: cross-boundary layout pins (all declared targets are 64-bit, so
   size_t is 8 bytes): the C declarations must stay byte-identical to the Rust
   #[repr(C)] originals. Any field addition, removal, or reorder breaks these. */
_Static_assert(
    sizeof(vyl_http1_server_body_event) == 40,
    "body event layout must match Rust ServerBodyEvent"
);
static void test_fixed_body_waits_for_continue_flush(void) {
    static const uint8_t request[] =
        "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 3\r\n"
        "Expect: 100-continue\r\n\r\n";
    static const uint8_t body[] = "abc";
    static const uint8_t reason[] = "Continue";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    uint8_t response[64];
    size_t headers_required = 0;
    size_t response_written = 0;
    size_t response_required = 0;
    uint32_t response_body_allowed = 0;
    uint32_t admission = UINT32_MAX;
    size_t response_pending = SIZE_MAX;
    size_t trailers_required = 0;
    vyl_http1_server_body_event event = {0};

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            4,
            &head,
            &headers_required
        ) == VYL_HTTP1_OK
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
        ) == VYL_HTTP1_OK
    );
    assert(admission == VYL_HTTP1_ADMISSION_SEND_CONTINUE);
    assert(
        vyl_http1_server_connection_read_body(
            connection,
            body,
            sizeof(body) - 1,
            0,
            NULL,
            0,
            &event,
            &trailers_required
        ) == VYL_HTTP1_BAD_STATE
    );
    assert(
        vyl_http1_server_connection_encode_response_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            4,
            100,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            NULL,
            0,
            VYL_HTTP1_RESPONSE_FRAMING_FIXED,
            0,
            0,
            0,
            response,
            sizeof(response),
            &response_written,
            &response_body_allowed,
            &response_required
        ) == VYL_HTTP1_OK
    );
    assert(response_body_allowed == 0);
    assert(vyl_http1_server_connection_continue_sent(connection) == VYL_HTTP1_BAD_STATE);
    assert(
        vyl_http1_server_connection_advance_response_write(
            connection,
            response_written,
            &response_pending
        ) == VYL_HTTP1_OK
    );
    assert(response_pending == 0);
    assert(vyl_http1_server_connection_continue_sent(connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_server_connection_read_body(
            connection,
            body,
            sizeof(body) - 1,
            0,
            NULL,
            0,
            &event,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_BODY_DATA);
    assert(event.consumed == sizeof(body) - 1);
    assert(memcmp(body + event.data.offset, "abc", event.data.length) == 0);
    assert(
        vyl_http1_server_connection_read_body(
            connection,
            NULL,
            0,
            0,
            NULL,
            0,
            &event,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_BODY_COMPLETE);
    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

static void test_chunked_trailer_capacity_retry(void) {
    static const uint8_t request[] =
        "POST /upload HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n";
    static const uint8_t allowed_name[] = "Digest";
    static const uint8_t body[] = "0\r\nDigest: ok\r\n\r\n";
    vyl_server_connection* connection = NULL;
    vyl_http1_header headers[4] = {0};
    vyl_http1_request_head head = {0};
    vyl_http1_span permitted[] = {{.offset = 0, .length = sizeof(allowed_name) - 1}};
    vyl_http1_header trailers[1] = {0};
    vyl_http1_server_body_event event = {0};
    size_t headers_required = 0;
    size_t trailers_required = 0;
    uint32_t admission = UINT32_MAX;

    assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
    assert(
        vyl_http1_server_connection_parse_head(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            4,
            &head,
            &headers_required
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_http1_server_connection_admit_body(
            connection,
            request,
            sizeof(request) - 1,
            headers,
            allowed_name,
            sizeof(allowed_name) - 1,
            permitted,
            1,
            1,
            &admission
        ) == VYL_HTTP1_OK
    );
    assert(admission == VYL_HTTP1_ADMISSION_READY);

    assert(
        vyl_http1_server_connection_read_body(
            connection,
            body,
            sizeof(body) - 1,
            0,
            NULL,
            0,
            &event,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_BODY_NEED_MORE);
    assert(event.consumed == 3);

    assert(
        vyl_http1_server_connection_read_body(
            connection,
            body + event.consumed,
            sizeof(body) - 1 - event.consumed,
            0,
            NULL,
            0,
            &event,
            &trailers_required
        ) == VYL_HTTP1_BUFFER_TOO_SMALL
    );
    assert(trailers_required == 1);

    const size_t trailer_offset = 3;
    assert(
        vyl_http1_server_connection_read_body(
            connection,
            body + trailer_offset,
            sizeof(body) - 1 - trailer_offset,
            0,
            trailers,
            1,
            &event,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_BODY_NEED_MORE);
    assert(
        vyl_http1_server_connection_read_body(
            connection,
            body + trailer_offset,
            sizeof(body) - 1 - trailer_offset,
            0,
            trailers,
            1,
            &event,
            &trailers_required
        ) == VYL_HTTP1_OK
    );
    assert(event.kind == VYL_HTTP1_BODY_TRAILERS);
    assert(event.trailer_count == 1);
    assert(memcmp(body + trailer_offset + trailers[0].name.offset, "Digest", 6) == 0);
    assert(memcmp(body + trailer_offset + trailers[0].value.offset, "ok", 2) == 0);

    assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);
}

int main(void) {
    test_fixed_body_waits_for_continue_flush();
    test_chunked_trailer_capacity_retry();
    return 0;
}
