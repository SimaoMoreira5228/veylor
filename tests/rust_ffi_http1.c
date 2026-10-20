#include "veylor_http1.h"

#include <assert.h>
#include <string.h>

_Static_assert(sizeof(vyl_http1_span) == sizeof(size_t) * 2,
               "span layout must match Rust");
_Static_assert(sizeof(vyl_http1_header) == sizeof(vyl_http1_span) * 2,
               "header layout must match Rust");

int main(void) {
  static const uint8_t request[] =
      "GET /health HTTP/1.1\r\nHost: example.test\r\nAccept: */*\r\n\r\n";
  static const uint8_t body_request[] =
      "POST /upload HTTP/1.1\r\nHost: example.test\r\nContent-Length: 5\r\n"
      "Expect: 100-continue\r\nConnection: close\r\n\r\n";
  static const uint8_t responses[] =
      "HTTP/1.1 100 Continue\r\n\r\n"
      "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789";
  static const uint8_t fixed_response[] =
      "HTTP/1.1 200 OK\r\nContent-Length: 10\r\n\r\n0123456789";
  static const uint8_t upgrade[] = "websocket";
  static const uint8_t upgrade_response[] =
      "HTTP/1.1 101 Switching Protocols\r\nConnection: upgrade\r\n"
      "Upgrade: websocket\r\n\r\n";
  static const size_t prefix_length =
      sizeof("GET /health HTTP/1.1\r\nHost: example.test\r\nAcc") - 1;
  vyl_server_connection *connection = NULL;
  vyl_client_connection *client = NULL;
  vyl_http1_header headers[4] = {0};
  vyl_http1_request_head head = {0};
  vyl_http1_response_head response_head = {0};
  size_t headers_required = SIZE_MAX;

  assert(vyl_http1_server_connection_new(&connection) == VYL_HTTP1_OK);
  assert(connection != NULL);
  assert(vyl_http1_server_connection_parse_head(
             connection, request, prefix_length, headers, 2, &head,
             &headers_required) == VYL_HTTP1_NEED_MORE);
  assert(headers_required == 0);

  assert(vyl_http1_server_connection_parse_head(
             connection, request, sizeof(request) - 1, headers, 2, &head,
             &headers_required) == VYL_HTTP1_OK);
  assert(head.target_form == VYL_HTTP1_TARGET_ORIGIN);
  assert(head.header_count == 2);
  assert(headers_required == 2);
  assert(head.method.length == 3);
  assert(memcmp(request + head.method.offset, "GET", 3) == 0);
  assert(memcmp(request + head.target.offset, "/health", 7) == 0);

  assert(vyl_http1_server_connection_reset(connection) == VYL_HTTP1_OK);
  assert(vyl_http1_server_connection_parse_head(
             connection, request, sizeof(request) - 1, headers, 1, &head,
             &headers_required) == VYL_HTTP1_BUFFER_TOO_SMALL);
  assert(headers_required == 2);
  assert(vyl_http1_server_connection_parse_head(
             connection, request, sizeof(request) - 1, headers, 2, &head,
             &headers_required) == VYL_HTTP1_OK);

  assert(vyl_http1_server_connection_reset(connection) == VYL_HTTP1_OK);
  assert(vyl_http1_server_connection_parse_head(
             connection, body_request, sizeof(body_request) - 1, headers, 4,
             &head, &headers_required) == VYL_HTTP1_OK);
  assert(head.content_length == 5);
  assert(head.flags == (VYL_HTTP1_HAS_CONTENT_LENGTH |
                        VYL_HTTP1_EXPECT_CONTINUE |
                        VYL_HTTP1_CONNECTION_CLOSE));
  assert(vyl_http1_server_connection_parse_head(
             connection, request, SIZE_MAX, headers, 4, &head,
             &headers_required) == VYL_HTTP1_INVALID_ARGUMENT);

  assert(vyl_http1_server_connection_destroy(connection) == VYL_HTTP1_OK);

  assert(vyl_http1_client_connection_new(
             VYL_HTTP1_REQUEST_HEAD, NULL, 0, &client) == VYL_HTTP1_OK);
  assert(vyl_http1_client_connection_parse_head(
             client, responses, SIZE_MAX, headers, 4, &response_head,
             &headers_required) == VYL_HTTP1_INVALID_ARGUMENT);
  assert(vyl_http1_client_connection_parse_head(
             client, responses, sizeof(responses) - 1, headers, SIZE_MAX,
             &response_head, &headers_required) == VYL_HTTP1_INVALID_ARGUMENT);
  assert(vyl_http1_client_connection_parse_head(
             client, responses, sizeof(responses) - 1, headers, 4,
             &response_head, &headers_required) == VYL_HTTP1_OK);
  assert(response_head.kind == VYL_HTTP1_RESPONSE_INFORMATIONAL);
  assert(response_head.status == 100);
  assert(vyl_http1_client_connection_advance(
             client, responses, sizeof(responses) - 1,
             response_head.end_offset) == VYL_HTTP1_OK);
  assert(vyl_http1_client_connection_parse_head(
             client, responses, sizeof(responses) - 1, headers, 4,
             &response_head, &headers_required) == VYL_HTTP1_OK);
  assert(response_head.kind == VYL_HTTP1_RESPONSE_FINAL);
  assert(response_head.status == 200);
  assert(response_head.body_kind == VYL_HTTP1_RESPONSE_BODY_NONE);
  assert((response_head.flags & VYL_HTTP1_RESPONSE_REUSABLE) != 0);
  assert(vyl_http1_client_connection_advance(
             client, responses, sizeof(responses) - 1,
             response_head.end_offset) == VYL_HTTP1_PROTOCOL_ERROR);
  assert(vyl_http1_client_connection_destroy(client) == VYL_HTTP1_OK);

  client = NULL;
  assert(vyl_http1_client_connection_new(
             VYL_HTTP1_REQUEST_OTHER, NULL, 0, &client) == VYL_HTTP1_OK);
  assert(vyl_http1_client_connection_parse_head(
             client, fixed_response, sizeof(fixed_response) - 1, headers, 4,
             &response_head, &headers_required) == VYL_HTTP1_OK);
  assert(response_head.body_kind == VYL_HTTP1_RESPONSE_BODY_FIXED);
  assert(response_head.body_length == 10);
  assert(memcmp(fixed_response + response_head.reason.offset, "OK", 2) == 0);
  assert(vyl_http1_client_connection_destroy(client) == VYL_HTTP1_OK);

  client = NULL;
  assert(vyl_http1_client_connection_new(
             VYL_HTTP1_REQUEST_OTHER, upgrade, sizeof(upgrade) - 1,
             &client) == VYL_HTTP1_OK);
  assert(vyl_http1_client_connection_parse_head(
             client, upgrade_response, sizeof(upgrade_response) - 1, headers,
             1, &response_head, &headers_required) ==
         VYL_HTTP1_BUFFER_TOO_SMALL);
  assert(headers_required == 2);
  assert(vyl_http1_client_connection_parse_head(
             client, upgrade_response, sizeof(upgrade_response) - 1, headers,
             4, &response_head, &headers_required) == VYL_HTTP1_OK);
  assert(response_head.kind == VYL_HTTP1_RESPONSE_UPGRADE);
  assert(response_head.status == 101);
  assert((response_head.flags & VYL_HTTP1_RESPONSE_CONNECTION_UPGRADE) != 0);
  assert(vyl_http1_client_connection_destroy(client) == VYL_HTTP1_OK);

  client = NULL;
  assert(vyl_http1_client_connection_new(
             VYL_HTTP1_REQUEST_OTHER, upgrade, SIZE_MAX, &client) ==
         VYL_HTTP1_INVALID_ARGUMENT);
  assert(client == NULL);

  assert(vyl_http1_client_connection_new(99, NULL, 0, &client) ==
         VYL_HTTP1_INVALID_ARGUMENT);
  assert(client == NULL);
  return 0;
}
