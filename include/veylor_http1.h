#ifndef VEYLOR_HTTP1_H
#define VEYLOR_HTTP1_H

#include <stddef.h>
#include <stdint.h>

#define VYL_HTTP1_MAX_REQUEST_FIELDS 100u

#ifdef __cplusplus
extern "C" {
#endif

typedef struct vyl_server_connection vyl_server_connection;
typedef struct vyl_client_connection vyl_client_connection;

typedef enum {
    VYL_HTTP1_OK = 0,
    VYL_HTTP1_NEED_MORE = 1,
    VYL_HTTP1_INVALID_ARGUMENT = 2,
    VYL_HTTP1_PROTOCOL_ERROR = 3,
    VYL_HTTP1_BUFFER_TOO_SMALL = 4,
    VYL_HTTP1_INTERNAL_ERROR = 5,
    VYL_HTTP1_CAPACITY = 6,
    VYL_HTTP1_WOULD_BLOCK = 7,
    VYL_HTTP1_BODY_TOO_LONG = 8,
    VYL_HTTP1_BAD_STATE = 9,
    VYL_HTTP1_WRITE_OVERRUN = 10,
    VYL_HTTP1_CANCELLED = 11
} vyl_http1_result;

typedef enum {
    VYL_HTTP1_TARGET_ORIGIN = 0,
    VYL_HTTP1_TARGET_ABSOLUTE = 1,
    VYL_HTTP1_TARGET_AUTHORITY = 2,
    VYL_HTTP1_TARGET_ASTERISK = 3
} vyl_http1_target_form;

typedef enum {
    VYL_HTTP1_REQUEST_HEAD = 0,
    VYL_HTTP1_REQUEST_CONNECT = 1,
    VYL_HTTP1_REQUEST_OTHER = 2
} vyl_http1_request_method;

typedef enum {
    VYL_HTTP1_RESPONSE_BODY_NONE = 0,
    VYL_HTTP1_RESPONSE_BODY_FIXED = 1,
    VYL_HTTP1_RESPONSE_BODY_CHUNKED = 2,
    VYL_HTTP1_RESPONSE_BODY_CLOSE_DELIMITED = 3
} vyl_http1_response_body_kind;

typedef enum {
    VYL_HTTP1_RESPONSE_INFORMATIONAL = 0,
    VYL_HTTP1_RESPONSE_FINAL = 1,
    VYL_HTTP1_RESPONSE_UPGRADE = 2
} vyl_http1_response_kind;

typedef struct {
    size_t offset;
    size_t length;
} vyl_http1_span;

typedef struct {
    vyl_http1_span name;
    vyl_http1_span value;
} vyl_http1_header;

typedef struct {
    const uint8_t* data;
    size_t length;
} vyl_http1_bytes;

typedef struct {
    vyl_http1_bytes name;
    vyl_http1_bytes value;
} vyl_http1_field;

typedef enum {
    VYL_HTTP1_REQUEST_BODY_NONE = 0,
    VYL_HTTP1_REQUEST_BODY_FIXED = 1,
    VYL_HTTP1_REQUEST_BODY_CHUNKED = 2
} vyl_http1_request_body_kind;

typedef enum {
    VYL_HTTP1_RESPONSE_FRAMING_FIXED = 0,
    VYL_HTTP1_RESPONSE_FRAMING_CHUNKED = 1,
    VYL_HTTP1_RESPONSE_FRAMING_CLOSE_DELIMITED = 2
} vyl_http1_response_framing;

typedef struct {
    vyl_http1_span method;
    vyl_http1_span target;
    vyl_http1_span authority;
    vyl_http1_span version;
    uint32_t target_form;
    size_t header_count;
    size_t length;
    uint64_t content_length;
    uint32_t flags;
} vyl_http1_request_head;

typedef struct {
    vyl_http1_span version;
    vyl_http1_span reason;
    uint16_t status;
    uint32_t body_kind;
    uint32_t kind;
    size_t header_count;
    size_t length;
    size_t end_offset;
    uint64_t body_length;
    uint32_t flags;
} vyl_http1_response_head;

typedef enum {
    VYL_HTTP1_CLIENT_NEED_MORE = 0,
    VYL_HTTP1_CLIENT_INFORMATIONAL = 1,
    VYL_HTTP1_CLIENT_HEAD = 2,
    VYL_HTTP1_CLIENT_BODY_DATA = 3,
    VYL_HTTP1_CLIENT_TRAILERS = 4,
    VYL_HTTP1_CLIENT_COMPLETE = 5,
    VYL_HTTP1_CLIENT_UPGRADE = 6
} vyl_http1_client_read_kind;

typedef struct {
    uint32_t kind;
    size_t consumed;
    vyl_http1_response_head response;
    vyl_http1_span body_data;
    size_t trailer_count;
    uint32_t can_reuse;
} vyl_http1_client_read_event;

typedef enum {
    VYL_HTTP1_ADMISSION_READY = 0,
    VYL_HTTP1_ADMISSION_SEND_CONTINUE = 1,
    VYL_HTTP1_ADMISSION_REJECTED = 2
} vyl_http1_body_admission;

typedef enum {
    VYL_HTTP1_BODY_NEED_MORE = 0,
    VYL_HTTP1_BODY_DATA = 1,
    VYL_HTTP1_BODY_TRAILERS = 2,
    VYL_HTTP1_BODY_COMPLETE = 3
} vyl_http1_body_event_kind;

typedef struct {
    uint32_t kind;
    size_t consumed;
    vyl_http1_span data;
    size_t trailer_count;
} vyl_http1_server_body_event;

enum {
    VYL_HTTP1_HAS_CONTENT_LENGTH = 1u,
    VYL_HTTP1_CHUNKED = 2u,
    VYL_HTTP1_EXPECT_CONTINUE = 4u,
    VYL_HTTP1_CONNECTION_CLOSE = 8u,
    VYL_HTTP1_CONNECTION_KEEP_ALIVE = 16u,
    VYL_HTTP1_CONNECTION_UPGRADE = 32u
};

enum {
    VYL_HTTP1_RESPONSE_REUSABLE = 1u,
    VYL_HTTP1_RESPONSE_CONNECTION_CLOSE = 2u,
    VYL_HTTP1_RESPONSE_CONNECTION_KEEP_ALIVE = 4u,
    VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE = 8u
};

vyl_http1_result vyl_http1_server_connection_new(vyl_server_connection** out_connection);
/* NOTE: input and header storage must keep the same base addresses until the
   parser completes or resets. Input bytes must stay alive and unchanged while
   returned spans are used. Input, header, output, and handle storage must not
   overlap. */
vyl_http1_result vyl_http1_server_connection_parse_head(
    vyl_server_connection* connection,
    const uint8_t* input,
    size_t input_length,
    vyl_http1_header* headers,
    size_t header_capacity,
    vyl_http1_request_head* out_head,
    size_t* out_headers_required
);
 
vyl_http1_result vyl_http1_server_connection_encode_response_head(
    vyl_server_connection* connection,
    const uint8_t* request_input,
    size_t request_input_length,
    const vyl_http1_header* request_headers,
    size_t request_header_capacity,
    uint16_t status,
    vyl_http1_bytes reason,
    const vyl_http1_field* fields,
    size_t field_count,
    uint32_t framing,
    uint64_t content_length,
    uint32_t request_body_complete,
    uint32_t response_close,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    uint32_t* out_body_allowed,
    size_t* out_required
);
/* NOTE: A successful response head is pending output too. Acknowledge every accepted byte count
   before encoding body bytes; keep each output buffer unchanged until fully acknowledged. */
vyl_http1_result vyl_http1_server_connection_advance_response_write(
    vyl_server_connection* connection,
    size_t transferred_bytes,
    size_t* out_pending
);
/* NOTE: Fixed and close-delimited bodies may be copied partially; chunked input is accepted only
   when its complete frame fits. Input is borrowed only for the call. */
vyl_http1_result vyl_http1_server_connection_encode_response_body(
    vyl_server_connection* connection,
    vyl_http1_bytes input,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_consumed,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Finishes a fixed or close-delimited body, or writes the final chunk and optional trailers.
 */
vyl_http1_result vyl_http1_server_connection_finish_response_body(
    vyl_server_connection* connection,
    const vyl_http1_field* trailers,
    size_t trailer_count,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Admission retains the request input and header buffers from parse_head, plus trailer-name
   data and spans, until reset or destruction. Use continue_sent only after a 100 Continue flushes.
 */
vyl_http1_result vyl_http1_server_connection_admit_body(
    vyl_server_connection* connection,
    const uint8_t* request_input,
    size_t request_input_length,
    const vyl_http1_header* request_headers,
    const uint8_t* trailer_name_data,
    size_t trailer_name_data_length,
    const vyl_http1_span* permitted_trailers,
    size_t permitted_trailer_count,
    uint32_t allowed,
    uint32_t* out_admission
);
vyl_http1_result vyl_http1_server_connection_continue_sent(vyl_server_connection* connection);
/* NOTE: Body spans refer to input. Preserve unread bytes using event.consumed; keep trailer storage
   at the same base until the body completes or the handle resets. */
vyl_http1_result vyl_http1_server_connection_read_body(
    vyl_server_connection* connection,
    const uint8_t* input,
    size_t input_length,
    uint32_t eof,
    vyl_http1_header* trailers,
    size_t trailer_capacity,
    vyl_http1_server_body_event* out_event,
    size_t* out_trailers_required
);
vyl_http1_result vyl_http1_server_connection_reset(vyl_server_connection* connection);
vyl_http1_result vyl_http1_server_connection_destroy(vyl_server_connection* connection);

vyl_http1_result vyl_http1_client_connection_new(
    uint32_t request_method,
    const uint8_t* requested_upgrade,
    size_t requested_upgrade_length,
    vyl_client_connection** out_connection
);
 
vyl_http1_result vyl_http1_client_connection_encode_request_head(
    vyl_client_connection* connection,
    vyl_http1_bytes method,
    vyl_http1_bytes target,
    const vyl_http1_field* fields,
    size_t field_count,
    uint32_t body_kind,
    uint64_t body_length,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
 
vyl_http1_result vyl_http1_client_connection_advance_request_write(
    vyl_client_connection* connection,
    size_t transferred_bytes,
    size_t* out_pending
);
 
vyl_http1_result vyl_http1_client_connection_encode_request_body(
    vyl_client_connection* connection,
    vyl_http1_bytes input,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
 
vyl_http1_result vyl_http1_client_connection_finish_request_body(
    vyl_client_connection* connection,
    const vyl_http1_field* trailers,
    size_t trailer_count,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
 
vyl_http1_result vyl_http1_client_connection_cancel(vyl_client_connection* connection);
/* NOTE: Keep requested_upgrade alive and unchanged until handle destruction.
   During an incomplete response head, retain the input base and bytes. Use
   event.consumed to keep the unread tail. Keep header/trailer buffers stable
   while the parser writes them. Input and writable buffers must not overlap;
   writable buffers must not overlap each other. Body spans point into input. */
vyl_http1_result vyl_http1_client_connection_read_response(
    vyl_client_connection* connection,
    const uint8_t* input,
    size_t input_length,
    uint32_t eof,
    vyl_http1_header* headers,
    size_t header_capacity,
    vyl_http1_header* trailers,
    size_t trailer_capacity,
    vyl_http1_client_read_event* out_event,
    size_t* out_headers_required,
    size_t* out_trailers_required
);
vyl_http1_result vyl_http1_client_connection_reset(vyl_client_connection* connection);
vyl_http1_result vyl_http1_client_connection_destroy(vyl_client_connection* connection);

#ifdef __cplusplus
}
#endif

#endif
