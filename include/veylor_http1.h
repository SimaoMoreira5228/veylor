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
    VYL_HTTP1_CANCELLED = 11,
    VYL_HTTP1_STALE_TOKEN = 12,
    VYL_HTTP1_DUPLICATE = 13,
    VYL_HTTP1_GONE = 14,
    VYL_HTTP1_BUSY = 15
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
/* NOTE: Stop transport IO and close the connection after cancel; this handle cannot be reset or
 * reused. */
vyl_http1_result vyl_http1_server_connection_cancel(vyl_server_connection* connection);
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

/* NOTE: these H2 bounds must stay identical to their veylor-client native
   constants; the cdylib side takes them from the same source. */
#define VYL_H2_MAX_ACTIONS 16u
#define VYL_H2_MAX_FIELDS 64u
#define VYL_H2_MAX_SETTINGS 8u
#define VYL_H2_MAX_HEADER_LIST_SIZE 2112u
#define VYL_H2_END_STREAM 1u
#define VYL_H2_NO_FIELD ((size_t)-1)

typedef struct vyl_h2_client vyl_h2_client;
typedef struct vyl_h2_server vyl_h2_server;

typedef enum {
    VYL_H2_ACTION_HEADERS = 0,
    VYL_H2_ACTION_DATA = 1,
    VYL_H2_ACTION_STREAM_RESET = 2,
    VYL_H2_ACTION_GOAWAY = 3,
    VYL_H2_ACTION_SETTINGS_ACK_REQUIRED = 4,
    VYL_H2_ACTION_PING_ACK = 5,
    VYL_H2_ACTION_SEND_RESET = 6,
    VYL_H2_ACTION_CONNECTION_ERROR = 7
} vyl_h2_action_kind;

typedef enum {
    VYL_H2_RESPONSE_INFORMATIONAL = 0,
    VYL_H2_RESPONSE_FINAL = 1,
    VYL_H2_RESPONSE_TRAILERS = 2
} vyl_h2_response_section_kind;

typedef enum {
    VYL_H2_PHASE_AWAITING_HEADERS = 0,
    VYL_H2_PHASE_CONTENT = 1,
    VYL_H2_PHASE_COMPLETE = 2
} vyl_h2_response_phase;

typedef enum {
    VYL_H2_INDEXED = 0,
    VYL_H2_INCREMENTAL = 1,
    VYL_H2_WITHOUT_INDEXING = 2,
    VYL_H2_NEVER_INDEXED = 3
} vyl_h2_representation;

typedef struct {
    vyl_http1_bytes name;
    vyl_http1_bytes value;
    uint32_t representation;
    uint32_t huffman_name;
    uint32_t huffman_value;
} vyl_h2_field_to_encode;

typedef struct {
    uint32_t id;
    uint32_t value;
} vyl_h2_setting;

typedef struct {
    size_t name_offset;
    size_t name_length;
    size_t value_offset;
    size_t value_length;
} vyl_h2_field;

typedef struct {
    uint32_t kind;
    uint32_t stream_id;
    uint32_t flags;
    uint32_t error_code;
    size_t data_offset;
    size_t data_length;
    size_t field_start;
    size_t field_count;
    uint8_t ping[8];
} vyl_h2_action;

typedef struct {
    uint32_t kind;
    uint16_t status;
    uint16_t phase;
    uint64_t content_length;
    uint32_t has_content_length;
    uint32_t end_stream;
} vyl_h2_response_class;

typedef struct {
    size_t method_field;
    size_t scheme_field;
    size_t authority_field;
    size_t path_field;
    uint64_t content_length;
    uint32_t has_content_length;
} vyl_h2_validated_request;

/* NOTE: Initial handle storage bounds: 32 streams, 4 KiB header block and
   HPACK scratch each way, 32-entry HPACK tables, 8 queued settings. These are
   initial values, not promises. One encoded frame is pending at a time;
   acknowledge every written byte count before the next send call. Poll copies
   header and data bytes into caller storage; field entries index the caller's
   field array and data spans index the caller's data buffer. Classify or
   accept reported headers before the next poll overwrites them. */
vyl_http1_result vyl_h2_client_new(vyl_h2_client** out_handle);
vyl_http1_result vyl_h2_client_destroy(vyl_h2_client* handle);
/* NOTE: Stop transport IO and close the connection after cancel; the handle
 * cannot be reused. */
vyl_http1_result vyl_h2_client_cancel(vyl_h2_client* handle);
/* NOTE: Copies the 24-byte client connection preface. Keep output unchanged
    until its bytes are transmitted. */
vyl_http1_result vyl_h2_client_connection_preface(
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: SETTINGS parameters are copied before return. A value of 1 for
    ENABLE_PUSH or a MAX_HEADER_LIST_SIZE above VYL_H2_MAX_HEADER_LIST_SIZE
    rejects. Missing values default to ENABLE_PUSH=0 and the field-list cap.
    Local settings take effect after the encoded bytes are acknowledged. */
vyl_http1_result vyl_h2_client_send_settings(
    vyl_h2_client* handle,
    const vyl_h2_setting* parameters,
    size_t parameter_count,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Acknowledge every accepted byte count before the next send call; keep
    each output buffer unchanged until fully acknowledged. */
vyl_http1_result
vyl_h2_client_acknowledge(vyl_h2_client* handle, size_t transferred_bytes, size_t* out_pending);
vyl_http1_result vyl_h2_client_send_settings_ack(
    vyl_h2_client* handle,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
vyl_http1_result vyl_h2_client_send_ping_ack(
    vyl_h2_client* handle,
    const uint8_t opaque_data[8],
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Grants matching receive credit alongside the WINDOW_UPDATE frame. */
vyl_http1_result vyl_h2_client_send_window_update(
    vyl_h2_client* handle,
    uint32_t stream_id,
    uint32_t increment,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Field bytes are copied or encoded before return. The originating
    method binds to the new stream for later classification. */
vyl_http1_result vyl_h2_client_start_request(
    vyl_h2_client* handle,
    const vyl_h2_field_to_encode* fields,
    size_t field_count,
    uint32_t end_stream,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    uint32_t* out_stream_id,
    size_t* out_required
);
/* NOTE: Partial input is accepted under flow-control or output limits; the
    end-stream flag tells whether the stream closed. */
vyl_http1_result vyl_h2_client_send_data(
    vyl_h2_client* handle,
    uint32_t stream_id,
    vyl_http1_bytes input,
    uint32_t end_stream,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_consumed,
    size_t* out_written,
    uint32_t* out_end_stream,
    size_t* out_required
);
vyl_http1_result vyl_h2_client_cancel_stream(
    vyl_h2_client* handle,
    uint32_t stream_id,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
vyl_http1_result vyl_h2_client_send_reset(
    vyl_h2_client* handle,
    uint32_t stream_id,
    uint32_t error_code,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
vyl_http1_result vyl_h2_client_goaway(
    vyl_h2_client* handle,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Feeds transport bytes and copies out whole actions. Consume
    out_consumed bytes after every call. On BUFFER_TOO_SMALL the reported
    bytes are consumed but the input must stay alive until a later call
    succeeds. Classify reported headers before the next poll. */
vyl_http1_result vyl_h2_client_poll_feed(
    vyl_h2_client* handle,
    const uint8_t* input,
    size_t input_length,
    size_t* out_consumed,
    vyl_h2_action* actions,
    size_t action_capacity,
    vyl_h2_field* fields,
    size_t field_capacity,
    uint8_t* data,
    size_t data_capacity,
    size_t* out_actions,
    size_t* out_fields,
    size_t* out_actions_required,
    size_t* out_fields_required,
    size_t* out_data_required,
    uint32_t* out_connection_error
);
/* NOTE: header_start/header_count must name a range from the most recent poll;
    call before the next poll. */
vyl_http1_result vyl_h2_client_classify_headers(
    vyl_h2_client* handle,
    uint32_t stream_id,
    size_t header_start,
    size_t header_count,
    uint32_t end_stream,
    vyl_h2_response_class* out_class
);
/* NOTE: Report each data payload length exactly once. */
vyl_http1_result vyl_h2_client_account_data(
    vyl_h2_client* handle,
    uint32_t stream_id,
    size_t data_length,
    uint32_t end_stream,
    uint32_t* out_phase
);

vyl_http1_result vyl_h2_server_new(vyl_h2_server** out_handle);
vyl_http1_result vyl_h2_server_destroy(vyl_h2_server* handle);
/* NOTE: Stop transport IO and close the connection after cancel; the handle
 * cannot be reused. */
vyl_http1_result vyl_h2_server_cancel(vyl_h2_server* handle);
/* NOTE: SETTINGS parameters are copied before return. Missing
    MAX_HEADER_LIST_SIZE defaults to VYL_H2_MAX_HEADER_LIST_SIZE. Larger values
    reject because the server field table is fixed. Local settings take effect
    after the encoded bytes are acknowledged. */
vyl_http1_result vyl_h2_server_send_settings(
    vyl_h2_server* handle,
    const vyl_h2_setting* parameters,
    size_t parameter_count,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Acknowledge every accepted byte count before the next send call; keep
    each output buffer unchanged until fully acknowledged. */
vyl_http1_result
vyl_h2_server_acknowledge(vyl_h2_server* handle, size_t transferred_bytes, size_t* out_pending);
vyl_http1_result vyl_h2_server_send_settings_ack(
    vyl_h2_server* handle,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
vyl_http1_result vyl_h2_server_send_ping_ack(
    vyl_h2_server* handle,
    const uint8_t opaque_data[8],
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Grants matching receive credit alongside the WINDOW_UPDATE frame. */
vyl_http1_result vyl_h2_server_send_window_update(
    vyl_h2_server* handle,
    uint32_t stream_id,
    uint32_t increment,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Feeds transport bytes and copies out whole actions. Consume
    out_consumed bytes after every call. On BUFFER_TOO_SMALL the reported
    bytes are consumed but the input must stay alive until a later call
    succeeds. Accept reported headers before the next poll. */
vyl_http1_result vyl_h2_server_poll_feed(
    vyl_h2_server* handle,
    const uint8_t* input,
    size_t input_length,
    size_t* out_consumed,
    vyl_h2_action* actions,
    size_t action_capacity,
    vyl_h2_field* fields,
    size_t field_capacity,
    uint8_t* data,
    size_t data_capacity,
    size_t* out_actions,
    size_t* out_fields,
    size_t* out_actions_required,
    size_t* out_fields_required,
    size_t* out_data_required,
    uint32_t* out_connection_error
);
/* NOTE: header_start/header_count must name a range from the most recent poll;
    call before the next poll. Reported field indices address the caller's
    field array from that same poll one-to-one. */
vyl_http1_result vyl_h2_server_accept_request(
    vyl_h2_server* handle,
    uint32_t stream_id,
    size_t header_start,
    size_t header_count,
    vyl_h2_validated_request* out_request
);
/* NOTE: Field bytes are copied or encoded before return. */
vyl_http1_result vyl_h2_server_send_response(
    vyl_h2_server* handle,
    uint32_t stream_id,
    const vyl_h2_field_to_encode* fields,
    size_t field_count,
    uint32_t end_stream,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
/* NOTE: Partial input is accepted under flow-control or output limits; the
    end-stream flag tells whether the stream closed. */
vyl_http1_result vyl_h2_server_send_data(
    vyl_h2_server* handle,
    uint32_t stream_id,
    vyl_http1_bytes input,
    uint32_t end_stream,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_consumed,
    size_t* out_written,
    uint32_t* out_end_stream,
    size_t* out_required
);
vyl_http1_result vyl_h2_server_refuse(
    vyl_h2_server* handle,
    uint32_t stream_id,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
vyl_http1_result vyl_h2_server_send_reset(
    vyl_h2_server* handle,
    uint32_t stream_id,
    uint32_t error_code,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);
vyl_http1_result vyl_h2_server_goaway_drain(
    vyl_h2_server* handle,
    uint32_t last_stream_id,
    uint8_t* output,
    size_t output_capacity,
    size_t* out_written,
    size_t* out_required
);

#define VYL_H3_CLIENT_ABI_VERSION 1u

typedef struct vyl_h3_client vyl_h3_client;
typedef struct vyl_h3_response vyl_h3_response;

/* NOTE: address is a numeric IPv4/IPv6 string and root_cert is a DER certificate
   chain. Requests block until the response completes; every returned span points
   into the owning response and stays valid until vyl_h3_response_destroy. */
typedef struct {
    uint32_t abi_version;
    uint32_t reserved0;
    size_t struct_size;
    vyl_http1_bytes address;
    vyl_http1_bytes server_name;
    vyl_http1_bytes root_cert;
    uint16_t port;
    uint16_t reserved1;
    uint32_t reserved2;
    uint64_t idle_timeout_ns;
    uint64_t max_response_body;
} vyl_h3_client_config;

_Static_assert(sizeof(vyl_h3_client_config) == 88, "vyl_h3_client_config size");
_Static_assert(offsetof(vyl_h3_client_config, struct_size) == 8, "vyl_h3_client_config struct_size");
_Static_assert(offsetof(vyl_h3_client_config, address) == 16, "vyl_h3_client_config address");
_Static_assert(offsetof(vyl_h3_client_config, port) == 64, "vyl_h3_client_config port");
_Static_assert(offsetof(vyl_h3_client_config, idle_timeout_ns) == 72, "vyl_h3_client_config idle_timeout_ns");
_Static_assert(offsetof(vyl_h3_client_config, max_response_body) == 80, "vyl_h3_client_config max_response_body");

vyl_http1_result vyl_h3_client_new(
    const vyl_h3_client_config* config,
    vyl_h3_client** out_client
);
vyl_http1_result vyl_h3_client_request(
    vyl_h3_client* client,
    vyl_http1_bytes method,
    vyl_http1_bytes url,
    vyl_http1_bytes body,
    vyl_h3_response** out_response
);
vyl_http1_result vyl_h3_client_destroy(vyl_h3_client* client);

uint16_t vyl_h3_response_status(const vyl_h3_response* response);
size_t vyl_h3_response_header_count(const vyl_h3_response* response);
vyl_http1_result vyl_h3_response_header(
    const vyl_h3_response* response,
    size_t index,
    vyl_http1_bytes* out_name,
    vyl_http1_bytes* out_value
);
vyl_http1_result vyl_h3_response_body(
    const vyl_h3_response* response,
    vyl_http1_bytes* out_body
);
vyl_http1_result vyl_h3_response_destroy(vyl_h3_response* response);

#define VYL_MANAGED_STATIC_ABI_VERSION 2u

typedef struct vyl_managed_static_server vyl_managed_static_server;

typedef struct {
    uint32_t abi_version;
    size_t struct_size;
    uint64_t request_head_timeout_ns;
    uint64_t body_progress_timeout_ns;
    uint64_t response_write_timeout_ns;
    uint64_t keep_alive_idle_timeout_ns;
    uint64_t shutdown_grace_ns;
    uint64_t drain_idle_linger_ns;
    uint64_t max_accepts_per_turn;
    uint64_t max_events_per_turn;
    uint64_t byte_budget_per_turn;
    int32_t poll_wait_ms;
    uint16_t port;
    uint32_t reserved;
    vyl_http1_bytes address;
    uint64_t max_connections;
    uint64_t worker_count;
} vyl_managed_static_config;

typedef struct {
    uint64_t completed;
    uint64_t peer_closed;
    uint64_t reset;
    uint64_t timeout;
    uint64_t protocol_error;
    uint64_t response_overflow;
    uint64_t transport_error;
    uint64_t shutdown;
} vyl_managed_static_close_counts;

/* NOTE: address is a numeric IPv4/IPv6 string; an empty value binds 127.0.0.1.
   max_connections and worker_count use 0 for the configured default. Register routes
   before start; start freezes them. Destroy is valid only before start or after join. */
vyl_http1_result vyl_managed_static_new(
    const vyl_managed_static_config* config,
    vyl_managed_static_server** out_server
);
vyl_http1_result vyl_managed_static_destroy(vyl_managed_static_server* server);
/* NOTE: Input bytes are copied before return; nothing is retained. */
vyl_http1_result vyl_managed_static_register_static_route(
    vyl_managed_static_server* server,
    vyl_http1_bytes method,
    vyl_http1_bytes path,
    uint16_t status,
    vyl_http1_bytes reason,
    vyl_http1_bytes body
);
/* NOTE: Same copy rule as above. The cap bounds request-body drain work: zero admits no body
   bytes, UINT64_MAX leaves the route uncapped except for input-buffer and deadline stops. */
vyl_http1_result vyl_managed_static_register_static_route_with_body_cap(
    vyl_managed_static_server* server,
    vyl_http1_bytes method,
    vyl_http1_bytes path,
    uint16_t status,
    vyl_http1_bytes reason,
    vyl_http1_bytes body,
    uint64_t max_body_bytes
);
/* NOTE: Same copy rule as above; the body precompresses once and serves gzip by Accept-Encoding. */
vyl_http1_result vyl_managed_static_register_static_route_with_gzip(
    vyl_managed_static_server* server,
    vyl_http1_bytes method,
    vyl_http1_bytes path,
    uint16_t status,
    vyl_http1_bytes reason,
    vyl_http1_bytes body
);
/* NOTE: File mounts serve regular files beneath an absolute root at a slash-bounded prefix
   (e.g. "/assets/"); GET/HEAD only, exact static routes win on overlap. Prefix bytes are
   copied and the root path is converted to owned storage before return; the root crosses
   as explicit-length bytes with no NUL assumption (byte-faithful on Unix, UTF-8 elsewhere).
   Register before start; bad prefixes, non-absolute/missing/non-directory roots, and
   duplicates fail with INVALID_ARGUMENT, full tables with CAPACITY, post-start with BAD_STATE. */
vyl_http1_result vyl_managed_static_register_file_mount(
    vyl_managed_static_server* server,
    vyl_http1_bytes prefix,
    vyl_http1_bytes root
);
/* NOTE: Managed routes park matched requests for a host dispatch call; over-cap declarations
   reject with 413 like static routes. Same copy and cap rules as above; register before start,
   duplicates fail. */
vyl_http1_result vyl_managed_static_register_managed_route(
    vyl_managed_static_server* server,
    vyl_http1_bytes method,
    vyl_http1_bytes path,
    uint64_t max_body_bytes
);
/* NOTE: Same park and copy rules as above, plus a per-route trailer allowlist copied before
   return: at most 32 names, each 1-128 bytes. Over-count, empty, or over-long names reject;
   duplicates (including against other managed or static routes) fail. An empty allowlist
   admits no trailer fields. */
#define VYL_MANAGED_STATIC_MAX_TRAILER_NAMES 32u
#define VYL_MANAGED_STATIC_MAX_TRAILER_NAME_BYTES 128u
#define VYL_MANAGED_STATIC_MAX_RESPONSE_TRAILERS 8u
#define VYL_MANAGED_STATIC_MAX_RESPONSE_TRAILER_BYTES 1024u
vyl_http1_result vyl_managed_static_register_managed_route_with_trailers(
    vyl_managed_static_server* server,
    vyl_http1_bytes method,
    vyl_http1_bytes path,
    uint64_t max_body_bytes,
    const vyl_http1_bytes* trailer_names,
    size_t trailer_count
);
/* NOTE: Stages the cleartext HTTP/2 listener port for start; the port binds at start beside
   the H1 listener. Zero binds an ephemeral loopback port, any other port binds fixed; the
   last call wins. Valid only before start; post-start reports BAD_STATE. */
vyl_http1_result vyl_managed_static_enable_h2c(vyl_managed_static_server* server, uint16_t port);
/* NOTE: Stages DER certificate/key bytes plus the per-worker handshake budget for
   start; bytes are copied at call time. Zero budget or empty inputs reject; the
   dedicated port binds loopback TLS at start beside the H1 listener. Valid only
   before start; post-start reports BAD_STATE. */
/* NOTE: Stages the HTTP/3 listener port for start; requires TLS enabled in the same
   config. Zero binds an ephemeral loopback port. Valid only before start. */
vyl_http1_result vyl_managed_static_enable_h3(vyl_managed_static_server* server, uint16_t port);
vyl_http1_result vyl_managed_static_enable_tls(
    vyl_managed_static_server* server,
    const uint8_t* cert,
    size_t cert_len,
    const uint8_t* key,
    size_t key_len,
    size_t max_handshakes
);
/* NOTE: Swaps the live TLS identity without restarting; staged material applies on the
   next worker turn while established connections keep serving. Running-only; pre-start
   and empty inputs report as for enable. */
vyl_http1_result vyl_managed_static_rotate_tls(
    vyl_managed_static_server* server,
    const uint8_t* cert,
    size_t cert_len,
    const uint8_t* key,
    size_t key_len
);
/* NOTE: Stages the worker count for start against the server cap of 16 workers: zero and
   counts past 16 report INVALID_ARGUMENT. One keeps the single-worker path; more workers
   bind reuse-port listeners at start. The last call wins. Valid only before start;
   post-start reports BAD_STATE. */
vyl_http1_result
vyl_managed_static_set_worker_count(vyl_managed_static_server* server, size_t count);
vyl_http1_result vyl_managed_static_start(vyl_managed_static_server* server);
vyl_http1_result vyl_managed_static_stop_accepting(vyl_managed_static_server* server);
vyl_http1_result vyl_managed_static_stop_force(vyl_managed_static_server* server);
/* NOTE: Joining from inside a native handler reports BUSY without touching either pointer. */
vyl_http1_result vyl_managed_static_join(
    vyl_managed_static_server* server,
    vyl_managed_static_close_counts* out_counts
);
vyl_http1_result
vyl_managed_static_local_addr(vyl_managed_static_server* server, uint16_t* out_port);
/* NOTE: Reports the bound cleartext HTTP/2 loopback port; valid only while running with H2C
   enabled. Pre-start (the listener binds at start), H2C-disabled, and joined handles report
   BAD_STATE; null handles or outputs report INVALID_ARGUMENT. Ephemeral staging reports the
   bound port, multi-worker reuse-port the shared port. */
vyl_http1_result vyl_managed_static_h2c_addr(vyl_managed_static_server* server, uint16_t* out_port);
/* NOTE: Reports the bound HTTP/3 port; BAD_STATE before start, after join, or when H3 is
   not enabled. */
vyl_http1_result vyl_managed_static_h3_addr(vyl_managed_static_server* server, uint16_t* out_port);

enum {
    VYL_MANAGED_STATIC_EVENT_REQUEST = 0u,
    VYL_MANAGED_STATIC_EVENT_SHUTDOWN = 1u,
    VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE = 2u,
    VYL_MANAGED_STATIC_EVENT_WRITABLE = 3u,
    VYL_MANAGED_STATIC_EVENT_TUNNEL_DATA = 4u,
    VYL_MANAGED_STATIC_EVENT_TUNNEL_CLOSED = 5u
};

typedef struct {
    uint32_t worker_id;
    uint32_t slot;
    uint32_t request_seq;
    uint32_t stream_id;
    uint64_t worker_generation;
    uint64_t generation;
} vyl_managed_static_token;

typedef struct {
    uint32_t kind;
    vyl_managed_static_token token;
    uint64_t route_id;
    size_t method_len;
    size_t path_len;
    size_t method_offset;
    size_t path_offset;
} vyl_managed_static_event;

typedef struct {
    size_t copied;
    size_t events_required;
    size_t method_required;
    size_t path_required;
} vyl_managed_static_poll_report;

/* NOTE: Whole-event dequeue into caller buffers: the first pending event that does not fit
   reports its required capacities and stays queued; zero event capacity reports the pending
   depth. timeout_ns == 0 takes a single pass, UINT64_MAX waits. Valid only while running. */
vyl_http1_result vyl_managed_static_poll_events(
    vyl_managed_static_server* server,
    uint64_t timeout_ns,
    vyl_managed_static_event* events,
    size_t event_capacity,
    uint8_t* methods,
    size_t method_capacity,
    uint8_t* paths,
    size_t path_capacity,
    vyl_managed_static_poll_report* out_report
);
/* NOTE: One-shot bounded answer for a parked token: a repeat reports DUPLICATE, a token whose
   connection closed reports GONE, unknown or superseded tokens report STALE_TOKEN. The body
   is bounded inline; the server cap still applies. Valid only while running. */
vyl_http1_result vyl_managed_static_host_respond(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint16_t status,
    vyl_http1_bytes reason,
    vyl_http1_bytes body
);
/* NOTE: Same copy rule as host_respond, plus caller-owned name/value arrays of `count`
   entries; framing/date headers stay engine-owned and reject as INVALID_ARGUMENT. */
vyl_http1_result vyl_managed_static_host_respond_with_headers(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint16_t status,
    vyl_http1_bytes reason,
    const vyl_http1_bytes* names,
    const vyl_http1_bytes* values,
    size_t count,
    vyl_http1_bytes body
);
/* NOTE: Cancels a parked request; the owner worker closes the connection. A repeat or an
   unknown token reports STALE_TOKEN. Valid only while running. */
vyl_http1_result
vyl_managed_static_host_cancel(vyl_managed_static_server* server, vyl_managed_static_token token);
/* NOTE: Partial body copies are normal; out_pulled counts this call, out_end flags the last
   byte. A zero-capacity call queries completion without consuming. Valid only while running. */
vyl_http1_result vyl_managed_static_host_pull_body(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint8_t* out,
    size_t out_capacity,
    size_t* out_pulled,
    uint32_t* out_end
);

typedef struct {
    size_t fields;
    size_t names_copied;
    size_t values_copied;
    size_t names_required;
    size_t values_required;
} vyl_managed_static_trailer_report;

/* NOTE: Whole-set trailer copy with NUL-joined names and values, ready only after end-of-body;
   an earlier pull reports WOULD_BLOCK without touching the buffers. Copies happen only when
   both caller buffers fit; otherwise nothing copies and the report carries the required
   capacities for a retry. A repeat after respond reports DUPLICATE, a closed connection GONE,
   unknown or superseded tokens STALE_TOKEN. Valid only while running. */
vyl_http1_result vyl_managed_static_host_pull_trailers(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint8_t* names,
    size_t names_capacity,
    uint8_t* values,
    size_t values_capacity,
    vyl_managed_static_trailer_report* out_report
);
/* NOTE: Opens a chunked response stream for a parked token: a repeat reports DUPLICATE, a token
   whose connection closed GONE. 204/205/304 and bad reasons report INVALID_ARGUMENT without
   staging, so a later one-shot respond still works. Begin arms nothing: poll is-writable until
   the head flushes before writing. Valid only while running. */
vyl_http1_result vyl_managed_static_host_begin_stream(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint16_t status,
    vyl_http1_bytes reason
);
/* NOTE: Same as begin_stream plus caller-owned name/value arrays; framing stays engine-owned
   and rejects like the fixed variant. */
vyl_http1_result vyl_managed_static_host_begin_stream_with_headers(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint16_t status,
    vyl_http1_bytes reason,
    const vyl_http1_bytes* names,
    const vyl_http1_bytes* values,
    size_t count
);
/* NOTE: Stages one bounded chunk on an open stream. Each admitted write consumes the writable
   arm: a write with no arm reports BAD_STATE, a write racing a staged command WOULD_BLOCK.
   Empty chunks report INVALID_ARGUMENT; lengths pass through with no separate C cap — the
   server rejects past 16 KiB (or past the output frame fit) with BODY_TOO_LONG. Valid only
   while running. */
vyl_http1_result vyl_managed_static_host_write_stream(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    vyl_http1_bytes chunk
);
/* NOTE: Ends an open stream with the terminal chunk; the connection unparks for keep-alive
   reuse. A repeat end while the first is still queued reports DUPLICATE, after apply GONE.
   Ending a token with no open stream reports INVALID_ARGUMENT. Valid only while running. */
vyl_http1_result vyl_managed_static_host_end_stream(
    vyl_managed_static_server* server,
    vyl_managed_static_token token
);
/* NOTE: Ends an open stream with a validated trailer block; the connection unparks for
   keep-alive reuse. Whole-set discipline: every field validates before anything stages, so a
   bad name, bad value, prohibited name, over-count (>8), or over-byte (>1 KiB total) set
   reports INVALID_ARGUMENT with the stream still open. A repeat end while the first is still
   queued reports DUPLICATE, after apply GONE; ending a token with no open stream reports
   INVALID_ARGUMENT. An encoded terminal past the output fit reports BODY_TOO_LONG. The field
   array and every nonempty trailer byte range must be valid; zero count accepts NULL. Valid
   only while running. */
vyl_http1_result vyl_managed_static_host_end_stream_with_trailers(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    const vyl_http1_field* trailers,
    size_t trailer_count
);
/* NOTE: Queries whether a stream token currently admits a write: OK with 1 when a write would
   stage, OK with 0 for any other live-or-unknown token. Only a bad handle, a non-running
   server, or a zero-generation token reports BAD_STATE. Valid only while running. */
vyl_http1_result vyl_managed_static_host_is_writable(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint32_t* out_writable
);
/* NOTE: Turns a managed request into a raw byte tunnel. An HTTP/1.1 WebSocket request
   answers 101 with the computed accept key; an HTTP/2 extended-CONNECT stream answers 200.
   The server validates the handshake from the parked snapshot, so INVALID_ARGUMENT means the
   request was not a valid upgrade (and nothing was staged). */
vyl_http1_result vyl_managed_static_host_upgrade_websocket(
    vyl_managed_static_server* server,
    vyl_managed_static_token token
);
/* NOTE: Selects one subprotocol the client offered in Sec-WebSocket-Protocol; an unoffered,
   empty, or over-long token reports INVALID_ARGUMENT and changes nothing. */
vyl_http1_result vyl_managed_static_host_upgrade_websocket_with_protocol(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    vyl_http1_bytes protocol
);
/* NOTE: Tunnel data is opaque: reads copy staged bytes in order and report WOULD_BLOCK when
   none are ready, GONE once the connection closed. TUNNEL_DATA is an edge hint (coalesced per
   token), so drain with repeated reads until WOULD_BLOCK instead of one read per event. Writes
   stage up to the per-tunnel cap and report WOULD_BLOCK when it is full; the staged bytes flush
   before a close requested by host_tunnel_close. Valid only while running. */
vyl_http1_result vyl_managed_static_host_tunnel_read(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    uint8_t* destination,
    size_t capacity,
    size_t* out_read
);
vyl_http1_result vyl_managed_static_host_tunnel_write(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    vyl_http1_bytes bytes,
    size_t* out_accepted
);
vyl_http1_result vyl_managed_static_host_tunnel_close(
    vyl_managed_static_server* server,
    vyl_managed_static_token token
);

/* NOTE: Native (worker-executed) routes. The handler runs on the connection owner worker and
   must return promptly without calling back into server APIs; registry and dispatch entries
   fail while a handler runs. Joining from inside a handler reports BUSY (the worker would
   wait on itself): pass the handle through user_data to observe it. Request
   bytes are borrowed for the call; the engine copies reason/body into bounded scratch before
   returning, so handler memory only needs to outlive the call. Native routes win over static
   and file routes for the same method and path (any overlap rejects as a duplicate). The
   request carries framing (has_body/body_len) plus the body bytes when the full
   fixed-length body is buffered at invoke time: body is NULL with has_body set when the
   tail has not arrived yet, and NULL with has_body clear when there is no body. Check
   body before reading body_len bytes; the pump still drains declared bytes. Retaining
   method/path/body past return is UB. A nonzero return fails the request closed without
   a partial write. Handlers are nounwind: unwinding (C++ exceptions, Rust panics) aborts
   the process, so handlers must catch their own failures and return nonzero instead.
   user_data stores opaque and comes back verbatim on every call, never dereferenced; it
   must outlive the handle and its thread-safety is the host's contract. Register before
   start; method/path copy before return, null handlers reject, post-start rejects. */
typedef struct {
    const uint8_t* method;
    size_t method_len;
    const uint8_t* path;
    size_t path_len;
    uint8_t has_body;
    uint64_t body_len;
    const uint8_t* body;
} vyl_managed_static_native_request;

typedef struct {
    int32_t status;
    const uint8_t* reason;
    size_t reason_len;
    const uint8_t* body;
    size_t body_len;
} vyl_managed_static_native_response;

typedef int32_t (*vyl_managed_static_native_handler)(
    void* user_data,
    const vyl_managed_static_native_request* request,
    vyl_managed_static_native_response* response
);

_Static_assert(
    sizeof(vyl_managed_static_native_request) == 56,
    "native request twin must match Rust NativeRequestView"
);
_Static_assert(
    offsetof(vyl_managed_static_native_request, has_body) == 32,
    "native request has_body offset must match Rust"
);
_Static_assert(
    offsetof(vyl_managed_static_native_request, body_len) == 40,
    "native request body_len offset must match Rust"
);
_Static_assert(
    offsetof(vyl_managed_static_native_request, body) == 48,
    "native request body offset must match Rust"
);
_Static_assert(
    sizeof(vyl_managed_static_native_response) == 40,
    "native response twin must match Rust NativeResponseOut"
);
_Static_assert(
    offsetof(vyl_managed_static_native_response, reason) == 8,
    "native response reason offset must match Rust"
);
_Static_assert(
    offsetof(vyl_managed_static_native_response, body_len) == 32,
    "native response body_len offset must match Rust"
);

vyl_http1_result vyl_managed_static_register_native_route(
    vyl_managed_static_server* server,
    vyl_http1_bytes method,
    vyl_http1_bytes path,
    vyl_managed_static_native_handler handler,
    void* user_data
);
/* NOTE: Same copy rule as above; the handler body compresses per response when the
   route opts in and the client sends Accept-Encoding: gzip. */
vyl_http1_result vyl_managed_static_register_native_route_with_gzip(
    vyl_managed_static_server* server,
    vyl_http1_bytes method,
    vyl_http1_bytes path,
    vyl_managed_static_native_handler handler,
    void* user_data
);

#ifdef __cplusplus
}
#endif

#endif
