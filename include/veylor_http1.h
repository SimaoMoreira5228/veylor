#ifndef VEYLOR_HTTP1_H
#define VEYLOR_HTTP1_H

#include <stddef.h>
#include <stdint.h>

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
  VYL_HTTP1_INTERNAL_ERROR = 5
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

vyl_http1_result vyl_http1_server_connection_new(
    vyl_server_connection **out_connection);
/* NOTE: input and header storage must keep the same base addresses until the
   parser completes or resets. Input bytes must stay alive and unchanged while
   returned spans are used. Input, header, output, and handle storage must not
   overlap. */
vyl_http1_result vyl_http1_server_connection_parse_head(
    vyl_server_connection *connection, const uint8_t *input,
    size_t input_length, vyl_http1_header *headers, size_t header_capacity,
    vyl_http1_request_head *out_head, size_t *out_headers_required);
vyl_http1_result vyl_http1_server_connection_reset(
    vyl_server_connection *connection);
vyl_http1_result vyl_http1_server_connection_destroy(
    vyl_server_connection *connection);

vyl_http1_result vyl_http1_client_connection_new(
    uint32_t request_method, const uint8_t *requested_upgrade,
    size_t requested_upgrade_length, vyl_client_connection **out_connection);
/* NOTE: Keep requested_upgrade alive and unchanged until handle destruction.
   Input storage stays stable through advance/reset; header storage stays
   stable until advance/reset. Returned spans point into the input buffer.
   Input, header, output, and handle storage must not overlap. */
vyl_http1_result vyl_http1_client_connection_parse_head(
    vyl_client_connection *connection, const uint8_t *input,
    size_t input_length, vyl_http1_header *headers, size_t header_capacity,
    vyl_http1_response_head *out_head, size_t *out_headers_required);
vyl_http1_result vyl_http1_client_connection_advance(
    vyl_client_connection *connection, const uint8_t *input,
    size_t input_length, size_t next_offset);
vyl_http1_result vyl_http1_client_connection_reset(
    vyl_client_connection *connection);
vyl_http1_result vyl_http1_client_connection_destroy(
    vyl_client_connection *connection);

#ifdef __cplusplus
}
#endif

#endif
