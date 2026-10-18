#define _POSIX_C_SOURCE 200809L

#include "veylor.h"

#include <arpa/inet.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

_Static_assert(sizeof(vyl_config) == 24, "vyl_config layout changed");
_Static_assert(offsetof(vyl_config, shutdown_grace_ns) == 16,
               "vyl_config offset changed");
_Static_assert(sizeof(vyl_bytes) == 16, "vyl_bytes layout changed");
_Static_assert(sizeof(vyl_request_token) == 32,
               "vyl_request_token layout changed");
_Static_assert(offsetof(vyl_request_token, request_seq) == 8,
               "vyl_request_token request_seq offset changed");
_Static_assert(sizeof(vyl_host_event) == 72,
                "vyl_host_event layout changed");
_Static_assert(offsetof(vyl_host_event, body_ready) == 64,
               "vyl_host_event body_ready offset changed");
_Static_assert(offsetof(vyl_host_event, body_complete) == 68,
               "vyl_host_event body_complete offset changed");
_Static_assert(offsetof(vyl_host_event, token) == 8,
               "vyl_host_event.token offset changed");
_Static_assert(offsetof(vyl_host_event, method_offset) == 48,
               "vyl_host_event method offset changed");
_Static_assert(sizeof(vyl_batch_info) == 16, "vyl_batch_info layout changed");
_Static_assert(sizeof(vyl_header_ref) == 16,
               "vyl_header_ref layout changed");
_Static_assert(sizeof(vyl_trailer_info) == 16,
               "vyl_trailer_info layout changed");
_Static_assert(VYL_WOULD_BLOCK == 10, "vyl_result WOULD_BLOCK changed");
_Static_assert(VYL_STALE_TOKEN == 11, "vyl_result STALE_TOKEN changed");
_Static_assert(VYL_END_OF_BODY == 12, "vyl_result END_OF_BODY changed");
_Static_assert(VYL_HOST_BODY_AVAILABLE == 2,
               "vyl_host_event_kind BODY_AVAILABLE changed");
_Static_assert(sizeof(vyl_native_request) == 32,
               "vyl_native_request layout changed");
_Static_assert(sizeof(vyl_native_response) == 40,
               "vyl_native_response layout changed");
_Static_assert(offsetof(vyl_native_response, status) == 0,
               "vyl_native_response.status offset changed");

static int join_guard_result = -1;

static int32_t join_guard_handler(void *user_data,
                                  const vyl_native_request *request,
                                  vyl_native_response *response) {
  if (request == NULL || response == NULL) {
    return VYL_INVALID_ARGUMENT;
  }
  vyl_server *server = (vyl_server *)user_data;
  join_guard_result = vyl_server_join(server);

  static uint8_t guard_reason[] = "OK";
  static uint8_t guard_body[] = "guarded\n";
  response->status = 200;
  response->reason.data = guard_reason;
  response->reason.length = sizeof(guard_reason) - 1;
  response->body.data = guard_body;
  response->body.length = sizeof(guard_body) - 1;
  return VYL_OK;
}

static int native_calls = 0;
static char native_seen_method[16];
static char native_seen_path[64];

static int32_t native_hello_handler(void *user_data,
                                    const vyl_native_request *request,
                                    vyl_native_response *response) {
  if (user_data == NULL || request == NULL || response == NULL) {
    return VYL_INVALID_ARGUMENT;
  }
  int *calls = (int *)user_data;
  (*calls)++;

  size_t method_length = request->method.length < sizeof(native_seen_method) - 1
                             ? request->method.length
                             : sizeof(native_seen_method) - 1;
  size_t path_length = request->path.length < sizeof(native_seen_path) - 1
                           ? request->path.length
                           : sizeof(native_seen_path) - 1;
  memcpy(native_seen_method, request->method.data, method_length);
  native_seen_method[method_length] = '\0';
  memcpy(native_seen_path, request->path.data, path_length);
  native_seen_path[path_length] = '\0';

  static uint8_t native_reason[] = "OK";
  static uint8_t native_body[] = "served by native handler\n";
  response->status = 202;
  response->reason.data = native_reason;
  response->reason.length = sizeof(native_reason) - 1;
  response->body.data = native_body;
  response->body.length = sizeof(native_body) - 1;
  return VYL_OK;
}

static char echo_seen_path[64];

static int32_t native_echo_handler(void *user_data,
                                   const vyl_native_request *request,
                                   vyl_native_response *response) {
  if (request == NULL || response == NULL) {
    return VYL_INVALID_ARGUMENT;
  }
  (void)user_data;
  size_t path_length = request->path.length < sizeof(echo_seen_path) - 1
                           ? request->path.length
                           : sizeof(echo_seen_path) - 1;
  memcpy(echo_seen_path, request->path.data, path_length);
  echo_seen_path[path_length] = '\0';

  static uint8_t echo_reason[] = "OK";
  static uint8_t echo_body[128];
  int written = snprintf((char *)echo_body, sizeof(echo_body), "echo:%s\n",
                         echo_seen_path);
  if (written < 0 || (size_t)written >= sizeof(echo_body)) {
    return VYL_INVALID_ARGUMENT;
  }
  response->status = 200;
  response->reason.data = echo_reason;
  response->reason.length = sizeof(echo_reason) - 1;
  response->body.data = echo_body;
  response->body.length = (uint32_t)written;
  return VYL_OK;
}

static int poll_until(vyl_server *server, vyl_host_event *events,
                      uint32_t event_capacity, uint8_t *data,
                      uint32_t data_capacity, vyl_batch_info *batch,
                      uint32_t want_events, int32_t want_result) {
  int32_t result = VYL_OK;
  for (int i = 0; i < 200; i++) {
    memset(batch, 0, sizeof(*batch));
    result = vyl_server_poll_events(server, events, event_capacity, data,
                                    data_capacity, 0, batch);
    if (result == want_result &&
        (want_result != VYL_OK || batch->event_count == want_events)) {
      return 1;
    }
    if (result != VYL_OK && result != VYL_BUFFER_TOO_SMALL) {
      fprintf(stderr, "host poll failed: %d\n", result);
      return 0;
    }
    struct timespec delay = {.tv_sec = 0, .tv_nsec = 5000000};
    nanosleep(&delay, NULL);
  }
  fprintf(stderr, "host poll timed out waiting for %u events, last result %d\n",
          want_events, result);
  return 0;
}

static int expect_result(const char *operation, int32_t actual,
                         int32_t expected) {
  if (actual == expected) {
    return 1;
  }

  fprintf(stderr, "%s returned %d, expected %d\n", operation, actual, expected);
  return 0;
}

static int request_health(uint16_t port) {
  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("socket");
    return 0;
  }

  struct sockaddr_in address = {
      .sin_family = AF_INET,
      .sin_port = htons(port),
  };
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
    perror("connect");
    close(fd);
    return 0;
  }

  const char request[] = "GET /health HTTP/1.1\r\n"
                         "Host: localhost\r\n"
                         "Connection: close\r\n"
                         "\r\n";
  size_t sent = 0;
  while (sent < sizeof(request) - 1) {
    ssize_t count = send(fd, request + sent, sizeof(request) - 1 - sent, 0);
    if (count <= 0) {
      perror("send");
      close(fd);
      return 0;
    }
    sent += (size_t)count;
  }

  char response[2048];
  size_t received = 0;
  while (received < sizeof(response) - 1) {
    ssize_t count =
        recv(fd, response + received, sizeof(response) - 1 - received, 0);
    if (count == 0) {
      break;
    }
    if (count < 0) {
      perror("recv");
      close(fd);
      return 0;
    }
    received += (size_t)count;
  }
  close(fd);
  response[received] = '\0';

  if (strstr(response, "HTTP/1.1 200 OK\r\n") == NULL ||
      strstr(response, "C host says hello\n") == NULL) {
    fprintf(stderr, "unexpected response:\n%s\n", response);
    return 0;
  }

  return 1;
}

static int read_response_head(int fd, char *response, size_t cap,
                              size_t *header_length) {
  size_t received = 0;
  while (received < cap - 1) {
    ssize_t count = recv(fd, response + received, 1, 0);
    if (count <= 0) {
      return 0;
    }
    received += (size_t)count;
    if (received >= 4 &&
        memcmp(response + received - 4, "\r\n\r\n", 4) == 0) {
      break;
    }
  }
  response[received] = '\0';
  *header_length = received;
  return received < cap;
}

static int read_exact_body(int fd, char *response, size_t header_length,
                           size_t body_length) {
  size_t received = 0;
  while (received < body_length) {
    ssize_t count = recv(fd, response + header_length + received,
                         body_length - received, 0);
    if (count <= 0) {
      return 0;
    }
    received += (size_t)count;
  }
  response[header_length + received] = '\0';
  return 1;
}

static int expect_managed_response(int fd, const char *expected_head,
                                   const char *expected_body, int expect_eof,
                                   int close_fd) {
  char response[2048];
  size_t header_length = 0;
  if (!read_response_head(fd, response, sizeof(response), &header_length)) {
    fprintf(stderr, "managed response head missing\n");
    if (close_fd) {
      close(fd);
    }
    return 0;
  }
  if (strstr(response, expected_head) == NULL) {
    fprintf(stderr, "unexpected managed response head:\n%s\n", response);
    if (close_fd) {
      close(fd);
    }
    return 0;
  }
  long content_length = -1;
  const char *cl = strstr(response, "Content-Length:");
  if (cl != NULL) {
    content_length = strtol(cl + 15, NULL, 10);
  }
  size_t want_body = strlen(expected_body);
  if (content_length != (long)want_body) {
    fprintf(stderr, "managed content length %ld, want %zu\n", content_length,
            want_body);
    if (close_fd) {
      close(fd);
    }
    return 0;
  }
  if (!read_exact_body(fd, response, header_length, want_body)) {
    fprintf(stderr, "managed response body missing\n");
    if (close_fd) {
      close(fd);
    }
    return 0;
  }
  if (strcmp(response + header_length, expected_body) != 0) {
    fprintf(stderr, "unexpected managed response body:\n%s\n",
            response + header_length);
    if (close_fd) {
      close(fd);
    }
    return 0;
  }
  if (expect_eof) {
    char discard;
    ssize_t count = recv(fd, &discard, 1, 0);
    if (count != 0) {
      fprintf(stderr, "managed connection not closed, recv %zd\n", count);
      if (close_fd) {
        close(fd);
      }
      return 0;
    }
  }
  if (close_fd) {
    close(fd);
  }
  return 1;
}

int main(void) {
  vyl_config config = {
      .abi_version = VEYLOR_ABI_VERSION,
      .struct_size = sizeof(vyl_config),
      .worker_count = 2,
      .shutdown_grace_ns = 1000000000,
  };
  vyl_server *server = NULL;

  vyl_config invalid_config = config;
  invalid_config.abi_version += 1;
  int32_t result = vyl_server_create(&invalid_config, &server);
  if (!expect_result("invalid config", result, VYL_INVALID_ARGUMENT)) {
    return 1;
  }
  if (server != NULL) {
    fprintf(stderr, "failed create returned a server handle\n");
    return 1;
  }

  result = vyl_server_create(&config, &server);
  if (!expect_result("server create", result, VYL_OK)) {
    return 1;
  }

  uint8_t method[] = "GET";
  uint8_t path[] = "/health";
  uint8_t reason[] = "OK";
  uint8_t body[] = "C host says hello\n";
  vyl_bytes method_bytes = {method, sizeof(method) - 1};
  vyl_bytes path_bytes = {path, sizeof(path) - 1};
  vyl_bytes reason_bytes = {reason, sizeof(reason) - 1};
  vyl_bytes body_bytes = {body, sizeof(body) - 1};
  vyl_bytes invalid_body = {NULL, 1};
  result = vyl_server_route_static(server, method_bytes, path_bytes, 200,
                                   reason_bytes, invalid_body);
  if (!expect_result("invalid body pointer", result, VYL_INVALID_ARGUMENT)) {
    return 1;
  }

  result = vyl_server_route_static(server, method_bytes, path_bytes, 200,
                                   reason_bytes, body_bytes);
  if (!expect_result("static route", result, VYL_OK)) {
    return 1;
  }

  memset(method, 'X', sizeof(method) - 1);
  memset(path, 'X', sizeof(path) - 1);
  memset(reason, 'X', sizeof(reason) - 1);
  memset(body, 'X', sizeof(body) - 1);

  uint8_t native_method[] = "GET";
  uint8_t native_path[] = "/native";
  vyl_bytes native_method_bytes = {native_method, sizeof(native_method) - 1};
  vyl_bytes native_path_bytes = {native_path, sizeof(native_path) - 1};
  result = vyl_server_route_native(server, native_method_bytes,
                                   native_path_bytes, NULL, &native_calls);
  if (!expect_result("null native handler", result, VYL_INVALID_ARGUMENT)) {
    return 1;
  }

  result =
      vyl_server_route_native(server, native_method_bytes, native_path_bytes,
                              native_hello_handler, &native_calls);
  if (!expect_result("native route", result, VYL_OK)) {
    return 1;
  }

  result =
      vyl_server_route_native(server, native_method_bytes, native_path_bytes,
                              native_hello_handler, &native_calls);
  if (!expect_result("duplicate native route", result, VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  uint8_t clash_reason[] = "OK";
  uint8_t clash_body[] = "x";
  result = vyl_server_route_static(
      server, native_method_bytes, native_path_bytes, 200,
      (vyl_bytes){clash_reason, sizeof(clash_reason) - 1},
      (vyl_bytes){clash_body, sizeof(clash_body) - 1});
  if (!expect_result("static over native", result, VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  vyl_bytes health_method = {(const uint8_t *)"GET", 3};
  vyl_bytes health_path = {(const uint8_t *)"/health", 7};
  result = vyl_server_route_native(server, health_method, health_path,
                                   native_hello_handler, &native_calls);
  if (!expect_result("native over static", result, VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  uint8_t echo_method[] = "GET";
  uint8_t echo_path[] = "/echo/*rest";
  result = vyl_server_route_native(
      server, (vyl_bytes){echo_method, sizeof(echo_method) - 1},
      (vyl_bytes){echo_path, sizeof(echo_path) - 1}, native_echo_handler, NULL);
  if (!expect_result("dynamic native route", result, VYL_OK)) {
    return 1;
  }

  memset(native_method, 'X', sizeof(native_method) - 1);
  memset(native_path, 'X', sizeof(native_path) - 1);
  uint8_t guard_method[] = "GET";
  uint8_t guard_path[] = "/join-guard";
  result = vyl_server_route_native(
      server, (vyl_bytes){guard_method, sizeof(guard_method) - 1},
      (vyl_bytes){guard_path, sizeof(guard_path) - 1}, join_guard_handler,
      server);
  if (!expect_result("join guard route", result, VYL_OK)) {
    return 1;
  }

  uint8_t managed_method[] = "GET";
  uint8_t managed_path[] = "/managed";
  result = vyl_server_route_managed(
      server, (vyl_bytes){managed_method, sizeof(managed_method) - 1},
      (vyl_bytes){managed_path, sizeof(managed_path) - 1});
  if (!expect_result("managed route", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_route_managed(
      server, (vyl_bytes){managed_method, sizeof(managed_method) - 1},
      (vyl_bytes){managed_path, sizeof(managed_path) - 1});
  if (!expect_result("duplicate managed route", result, VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  uint8_t clash_method[] = "GET";
  uint8_t clash_path[] = "/health";
  result = vyl_server_route_managed(
      server, (vyl_bytes){clash_method, sizeof(clash_method) - 1},
      (vyl_bytes){clash_path, sizeof(clash_path) - 1});
  if (!expect_result("managed over static", result, VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  uint8_t upload_method[] = "POST";
  uint8_t upload_path[] = "/upload";
  result = vyl_server_route_managed(
      server, (vyl_bytes){upload_method, sizeof(upload_method) - 1},
      (vyl_bytes){upload_path, sizeof(upload_path) - 1});
  if (!expect_result("managed upload route", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_route_managed(
      server, (vyl_bytes){upload_method, sizeof(upload_method) - 1},
      (vyl_bytes){upload_path, sizeof(upload_path) - 1});
  if (!expect_result("duplicate managed upload route", result,
                     VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  uint8_t trailer_method[] = "POST";
  uint8_t trailer_path[] = "/trailers";
  uint8_t trailer_allow_0[] = "x-checksum";
  uint8_t trailer_allow_1[] = "x-signature";
  vyl_bytes trailer_names[] = {
      {trailer_allow_0, sizeof(trailer_allow_0) - 1},
      {trailer_allow_1, sizeof(trailer_allow_1) - 1},
  };
  result = vyl_server_route_managed_with_trailers(
      server, (vyl_bytes){trailer_method, sizeof(trailer_method) - 1},
      (vyl_bytes){trailer_path, sizeof(trailer_path) - 1}, trailer_names, 2);
  if (!expect_result("managed trailer route", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_route_managed(
      server, (vyl_bytes){trailer_method, sizeof(trailer_method) - 1},
      (vyl_bytes){trailer_path, sizeof(trailer_path) - 1});
  if (!expect_result("plain route over trailer route", result,
                     VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  uint8_t bad_trailer_name[] = "bad name";
  vyl_bytes bad_trailer_names[] = {
      {bad_trailer_name, sizeof(bad_trailer_name) - 1},
  };
  if (!expect_result("trailer route zero count",
                     vyl_server_route_managed_with_trailers(
                         server, (vyl_bytes){trailer_method, 4},
                         (vyl_bytes){trailer_path, 9}, trailer_names, 0),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("trailer route null names",
                     vyl_server_route_managed_with_trailers(
                         server, (vyl_bytes){trailer_method, 4},
                         (vyl_bytes){trailer_path, 9}, NULL, 1),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("trailer route bad name",
                     vyl_server_route_managed_with_trailers(
                         server, (vyl_bytes){trailer_method, 4},
                         (vyl_bytes){trailer_path, 9}, bad_trailer_names, 1),
                     VYL_INVALID_ARGUMENT)) {
    return 1;
  }
  {
    uint8_t many_names[33][8];
    vyl_bytes many_trailers[33];
    for (int i = 0; i < 33; i++) {
      snprintf((char *)many_names[i], sizeof(many_names[i]), "t%03d", i);
      many_trailers[i] = (vyl_bytes){many_names[i], 4};
    }
    if (!expect_result("trailer route over count",
                       vyl_server_route_managed_with_trailers(
                           server, (vyl_bytes){trailer_method, 4},
                           (vyl_bytes){trailer_path, 9}, many_trailers, 33),
                       VYL_INVALID_ARGUMENT)) {
      return 1;
    }
  }

  memset(echo_method, 'X', sizeof(echo_method) - 1);
  memset(echo_path, 'X', sizeof(echo_path) - 1);
  memset(guard_method, 'X', sizeof(guard_method) - 1);
  memset(guard_path, 'X', sizeof(guard_path) - 1);
  memset(managed_method, 'X', sizeof(managed_method) - 1);
  memset(managed_path, 'X', sizeof(managed_path) - 1);
  memset(clash_method, 'X', sizeof(clash_method) - 1);
  memset(clash_path, 'X', sizeof(clash_path) - 1);
  memset(upload_method, 'X', sizeof(upload_method) - 1);
  memset(upload_path, 'X', sizeof(upload_path) - 1);
  memset(trailer_method, 'X', sizeof(trailer_method) - 1);
  memset(trailer_path, 'X', sizeof(trailer_path) - 1);
  memset(trailer_allow_0, 'X', sizeof(trailer_allow_0) - 1);
  memset(trailer_allow_1, 'X', sizeof(trailer_allow_1) - 1);

  const uint8_t loopback[4] = {127, 0, 0, 1};
  uint16_t bound_port = 0;
  int32_t system_error = 0;
  result = vyl_server_listen(server, loopback, 0, &bound_port, &system_error);
  if (!expect_result("listen", result, VYL_OK)) {
    fprintf(stderr, "listen system error: %d\n", system_error);
    return 1;
  }
  if (bound_port == 0) {
    fprintf(stderr, "listen returned port zero\n");
    return 1;
  }
  result = vyl_server_start(server);
  if (!expect_result("start", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_destroy(server);
  if (!expect_result("destroy while running", result, VYL_BUSY)) {
    return 1;
  }
  if (!request_health(bound_port)) {
    return 1;
  }

  {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      perror("socket");
      return 1;
    }
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
      perror("connect");
      close(fd);
      return 1;
    }
    const char native_request[] = "GET /native HTTP/1.1\r\n"
                                  "Host: localhost\r\n"
                                  "Connection: close\r\n"
                                  "\r\n";
    size_t sent = 0;
    while (sent < sizeof(native_request) - 1) {
      ssize_t count =
          send(fd, native_request + sent, sizeof(native_request) - 1 - sent, 0);
      if (count <= 0) {
        perror("send");
        close(fd);
        return 1;
      }
      sent += (size_t)count;
    }
    char native_response[2048];
    size_t received = 0;
    while (received < sizeof(native_response) - 1) {
      ssize_t count = recv(fd, native_response + received,
                           sizeof(native_response) - 1 - received, 0);
      if (count == 0) {
        break;
      }
      if (count < 0) {
        perror("recv");
        close(fd);
        return 1;
      }
      received += (size_t)count;
    }
    close(fd);
    native_response[received] = '\0';
    if (strstr(native_response, "HTTP/1.1 202 OK\r\n") == NULL ||
        strstr(native_response, "served by native handler\n") == NULL) {
      fprintf(stderr, "unexpected native response:\n%s\n", native_response);
      return 1;
    }
  }
  if (native_calls != 1) {
    fprintf(stderr, "native handler ran %d times, expected 1\n", native_calls);
    return 1;
  }
  {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      perror("socket");
      return 1;
    }
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
      perror("connect");
      close(fd);
      return 1;
    }
    const char echo_request[] = "GET /echo/abc HTTP/1.1\r\n"
                                "Host: localhost\r\n"
                                "Connection: close\r\n"
                                "\r\n";
    size_t sent = 0;
    while (sent < sizeof(echo_request) - 1) {
      ssize_t count =
          send(fd, echo_request + sent, sizeof(echo_request) - 1 - sent, 0);
      if (count <= 0) {
        perror("send");
        close(fd);
        return 1;
      }
      sent += (size_t)count;
    }
    char echo_response[2048];
    size_t received = 0;
    while (received < sizeof(echo_response) - 1) {
      ssize_t count = recv(fd, echo_response + received,
                           sizeof(echo_response) - 1 - received, 0);
      if (count == 0) {
        break;
      }
      if (count < 0) {
        perror("recv");
        close(fd);
        return 1;
      }
      received += (size_t)count;
    }
    close(fd);
    echo_response[received] = '\0';
    if (strstr(echo_response, "HTTP/1.1 200 OK\r\n") == NULL ||
        strstr(echo_response, "echo:/echo/abc\n") == NULL) {
      fprintf(stderr, "unexpected echo response:\n%s\n", echo_response);
      return 1;
    }
  }
  if (strcmp(native_seen_method, "GET") != 0 ||
      strcmp(native_seen_path, "/native") != 0) {
    fprintf(stderr, "native handler saw %s %s\n", native_seen_method,
            native_seen_path);
    return 1;
  }

  int managed_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (managed_fd < 0) {
    perror("socket");
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(managed_fd, (struct sockaddr *)&address, sizeof(address)) !=
        0) {
      perror("connect");
      close(managed_fd);
      return 1;
    }
  }
  const char managed_request[] = "GET /managed HTTP/1.1\r\n"
                                 "Host: localhost\r\n"
                                 "\r\n";
  size_t managed_sent = 0;
  while (managed_sent < sizeof(managed_request) - 1) {
    ssize_t count = send(managed_fd, managed_request + managed_sent,
                         sizeof(managed_request) - 1 - managed_sent, 0);
    if (count <= 0) {
      perror("send");
      close(managed_fd);
      return 1;
    }
    managed_sent += (size_t)count;
  }

  vyl_host_event poll_events[4];
  uint8_t poll_data[256];
  vyl_batch_info batch = {0};
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(managed_fd);
    return 1;
  }
  if (batch.event_count != 1 || batch.events_required != 0) {
    fprintf(stderr, "unexpected batch counts %u/%u\n", batch.event_count,
            batch.events_required);
    close(managed_fd);
    return 1;
  }
  if (poll_events[0].kind != VYL_HOST_REQUEST || poll_events[0].route_id == 0) {
    fprintf(stderr, "unexpected polled event kind %u route %llu\n",
            poll_events[0].kind, (unsigned long long)poll_events[0].route_id);
    close(managed_fd);
    return 1;
  }
  if (poll_events[0].method_length != 3 ||
      memcmp(poll_data + poll_events[0].method_offset, "GET", 3) != 0 ||
      poll_events[0].path_length != 8 ||
      memcmp(poll_data + poll_events[0].path_offset, "/managed", 8) != 0) {
    fprintf(stderr, "unexpected polled method/path\n");
    close(managed_fd);
    return 1;
  }
  vyl_request_token managed_token = poll_events[0].token;

  memset(&batch, 0, sizeof(batch));
  result = vyl_server_poll_events(server, poll_events, 4, poll_data,
                                  sizeof(poll_data), 0, &batch);
  if (!expect_result("host poll empty", result, VYL_OK)) {
    close(managed_fd);
    return 1;
  }
  if (batch.event_count != 0) {
    fprintf(stderr, "expected empty poll, got %u\n", batch.event_count);
    close(managed_fd);
    return 1;
  }

  int managed_fd2 = socket(AF_INET, SOCK_STREAM, 0);
  if (managed_fd2 < 0) {
    perror("socket");
    close(managed_fd);
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(managed_fd2, (struct sockaddr *)&address, sizeof(address)) !=
        0) {
      perror("connect");
      close(managed_fd2);
      close(managed_fd);
      return 1;
    }
  }
  const char managed_request2[] = "GET /managed HTTP/1.1\r\n"
                                  "Host: localhost\r\n"
                                  "Connection: close\r\n"
                                  "\r\n";
  managed_sent = 0;
  while (managed_sent < sizeof(managed_request2) - 1) {
    ssize_t count = send(managed_fd2, managed_request2 + managed_sent,
                         sizeof(managed_request2) - 1 - managed_sent, 0);
    if (count <= 0) {
      perror("send");
      close(managed_fd2);
      close(managed_fd);
      return 1;
    }
    managed_sent += (size_t)count;
  }
  if (!poll_until(server, poll_events, 0, poll_data, sizeof(poll_data), &batch,
                  0, VYL_BUFFER_TOO_SMALL)) {
    close(managed_fd);
    return 1;
  }
  if (batch.events_required == 0) {
    fprintf(stderr, "expected required events\n");
    close(managed_fd);
    return 1;
  }

  if (!poll_until(server, poll_events, 4, poll_data, 1, &batch, 0,
                  VYL_BUFFER_TOO_SMALL)) {
    close(managed_fd);
    return 1;
  }
  if (batch.data_required != 11) {
    fprintf(stderr, "expected 11 data bytes required, got %u\n",
            batch.data_required);
    close(managed_fd);
    return 1;
  }

  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(managed_fd);
    return 1;
  }
  if (batch.event_count != 1) {
    fprintf(stderr, "expected drained event, got %u\n", batch.event_count);
    close(managed_fd);
    return 1;
  }
  close(managed_fd2);

  int upload_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (upload_fd < 0) {
    perror("socket");
    close(managed_fd);
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(upload_fd, (struct sockaddr *)&address, sizeof(address)) !=
        0) {
      perror("connect");
      close(upload_fd);
      close(managed_fd);
      return 1;
    }
  }
  const char upload_head[] = "POST /upload HTTP/1.1\r\n"
                             "Host: localhost\r\n"
                             "Content-Length: 10\r\n"
                             "Connection: close\r\n"
                             "\r\n"
                             "hello";
  size_t upload_sent = 0;
  while (upload_sent < sizeof(upload_head) - 1) {
    ssize_t count = send(upload_fd, upload_head + upload_sent,
                         sizeof(upload_head) - 1 - upload_sent, 0);
    if (count <= 0) {
      perror("send");
      close(upload_fd);
      close(managed_fd);
      return 1;
    }
    upload_sent += (size_t)count;
  }

  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  if (poll_events[0].kind != VYL_HOST_REQUEST ||
      poll_events[0].method_length != 4 ||
      memcmp(poll_data + poll_events[0].method_offset, "POST", 4) != 0 ||
      poll_events[0].path_length != 7 ||
      memcmp(poll_data + poll_events[0].path_offset, "/upload", 7) != 0) {
    fprintf(stderr, "unexpected upload request event\n");
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  vyl_request_token upload_token = poll_events[0].token;
  uint64_t upload_route = poll_events[0].route_id;

  uint8_t body_chunk[32];
  uint32_t body_count = 0;
  result = vyl_request_read(server, upload_token, body_chunk,
                            sizeof(body_chunk), &body_count);
  if (!expect_result("read before pull", result, VYL_WOULD_BLOCK)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  result = vyl_request_pull_body(server, upload_token, 1024);
  if (!expect_result("first body pull", result, VYL_OK)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  if (poll_events[0].kind != VYL_HOST_BODY_AVAILABLE ||
      poll_events[0].route_id != upload_route ||
      poll_events[0].body_ready != 5 || poll_events[0].body_complete != 0) {
    fprintf(stderr, "unexpected first body event: kind %u ready %u complete %u\n",
            poll_events[0].kind, poll_events[0].body_ready,
            poll_events[0].body_complete);
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  result = vyl_request_read(server, upload_token, body_chunk, 2, &body_count);
  if (!expect_result("partial read", result, VYL_OK) || body_count != 2 ||
      memcmp(body_chunk, "he", 2) != 0) {
    fprintf(stderr, "unexpected partial read: %d/%u\n", result, body_count);
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  result =
      vyl_request_read(server, upload_token, body_chunk, sizeof(body_chunk),
                       &body_count);
  if (!expect_result("drain staged", result, VYL_OK) || body_count != 3 ||
      memcmp(body_chunk, "llo", 3) != 0) {
    fprintf(stderr, "unexpected staged drain: %d/%u\n", result, body_count);
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  result = vyl_request_read(server, upload_token, body_chunk,
                            sizeof(body_chunk), &body_count);
  if (!expect_result("read awaiting remainder", result, VYL_WOULD_BLOCK)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }

  const char upload_tail[] = "world";
  upload_sent = 0;
  while (upload_sent < sizeof(upload_tail) - 1) {
    ssize_t count = send(upload_fd, upload_tail + upload_sent,
                         sizeof(upload_tail) - 1 - upload_sent, 0);
    if (count <= 0) {
      perror("send");
      close(upload_fd);
      close(managed_fd);
      return 1;
    }
    upload_sent += (size_t)count;
  }
  result = vyl_request_pull_body(server, upload_token, 1024);
  if (!expect_result("second body pull", result, VYL_OK)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  if (poll_events[0].kind != VYL_HOST_BODY_AVAILABLE ||
      poll_events[0].body_ready != 5 || poll_events[0].body_complete != 1) {
    fprintf(stderr, "unexpected final body event: kind %u ready %u complete %u\n",
            poll_events[0].kind, poll_events[0].body_ready,
            poll_events[0].body_complete);
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  result =
      vyl_request_read(server, upload_token, body_chunk, sizeof(body_chunk),
                       &body_count);
  if (!expect_result("final body read", result, VYL_END_OF_BODY) ||
      body_count != 5 || memcmp(body_chunk, "world", 5) != 0) {
    fprintf(stderr, "unexpected final read: %d/%u\n", result, body_count);
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  result = vyl_request_read(server, upload_token, body_chunk,
                            sizeof(body_chunk), &body_count);
  if (!expect_result("read after end", result, VYL_END_OF_BODY) ||
      body_count != 0) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }

  vyl_request_token stale_token = upload_token;
  stale_token.connection_generation += 1;
  result = vyl_request_read(server, stale_token, body_chunk,
                            sizeof(body_chunk), &body_count);
  if (!expect_result("stale body read", result, VYL_STALE_TOKEN)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  result = vyl_request_pull_body(server, stale_token, 1024);
  if (!expect_result("stale body pull", result, VYL_STALE_TOKEN)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  if (!expect_result("null pull server",
                     vyl_request_pull_body(NULL, upload_token, 1024),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("zero pull bytes",
                     vyl_request_pull_body(server, upload_token, 0),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("null read destination",
                     vyl_request_read(server, upload_token, NULL, 16,
                                      &body_count),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("zero read capacity",
                     vyl_request_read(server, upload_token, body_chunk, 0,
                                      &body_count),
                     VYL_INVALID_ARGUMENT)) {
    close(upload_fd);
    close(managed_fd);
    return 1;
  }
  uint8_t resp_reason[] = "OK";
  uint8_t resp_body[] = "hello back";
  result = vyl_response_start(server, upload_token, 200,
                              (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                              (vyl_bytes){resp_body, sizeof(resp_body) - 1});
  if (!expect_result("managed upload response", result, VYL_OK)) {
    close(managed_fd);
    return 1;
  }
  if (!expect_managed_response(upload_fd, "HTTP/1.1 200 OK\r\n",
                               "hello back", 1, 1)) {
    close(managed_fd);
    return 1;
  }
  result = vyl_response_start(server, upload_token, 200,
                              (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                              (vyl_bytes){resp_body, sizeof(resp_body) - 1});
  if (!expect_result("duplicate managed response", result, VYL_STALE_TOKEN)) {
    close(managed_fd);
    return 1;
  }

  uint8_t bad_reason[] = "O\r\nK";
  uint8_t big_body[769];
  memset(big_body, 'x', sizeof(big_body));
  if (!expect_result("bad response status",
                     vyl_response_start(server, managed_token, 99,
                                        (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                                        (vyl_bytes){resp_body, sizeof(resp_body) - 1}),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("bad response reason",
                     vyl_response_start(server, managed_token, 200,
                                        (vyl_bytes){bad_reason, sizeof(bad_reason) - 1},
                                        (vyl_bytes){resp_body, sizeof(resp_body) - 1}),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("oversized response body",
                     vyl_response_start(server, managed_token, 200,
                                        (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                                        (vyl_bytes){big_body, sizeof(big_body)}),
                     VYL_INVALID_ARGUMENT) ||
      !expect_result("null response server",
                     vyl_response_start(NULL, managed_token, 200,
                                        (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                                        (vyl_bytes){resp_body, sizeof(resp_body) - 1}),
                     VYL_INVALID_ARGUMENT)) {
    close(managed_fd);
    return 1;
  }

  uint8_t managed_resp_body[] = "managed hello\n";
  result = vyl_response_start(
      server, managed_token, 200, (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
      (vyl_bytes){managed_resp_body, sizeof(managed_resp_body) - 1});
  if (!expect_result("managed get response", result, VYL_OK)) {
    close(managed_fd);
    return 1;
  }
  if (!expect_managed_response(managed_fd, "HTTP/1.1 200 OK\r\n",
                               "managed hello\n", 0, 0)) {
    close(managed_fd);
    return 1;
  }
  result = vyl_request_cancel(server, managed_token);
  if (!expect_result("cancel after response", result, VYL_STALE_TOKEN)) {
    close(managed_fd);
    return 1;
  }

  const char managed_request_again[] = "GET /managed HTTP/1.1\r\n"
                                       "Host: localhost\r\n"
                                       "\r\n";
  managed_sent = 0;
  while (managed_sent < sizeof(managed_request_again) - 1) {
    ssize_t count = send(managed_fd, managed_request_again + managed_sent,
                         sizeof(managed_request_again) - 1 - managed_sent, 0);
    if (count <= 0) {
      perror("send");
      close(managed_fd);
      return 1;
    }
    managed_sent += (size_t)count;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(managed_fd);
    return 1;
  }
  if (poll_events[0].kind != VYL_HOST_REQUEST ||
      poll_events[0].method_length != 3 ||
      memcmp(poll_data + poll_events[0].method_offset, "GET", 3) != 0) {
    fprintf(stderr, "unexpected reused managed request event\n");
    close(managed_fd);
    return 1;
  }
  uint8_t again_body[] = "again\n";
  vyl_request_token reuse_token = poll_events[0].token;
  if (reuse_token.request_seq == managed_token.request_seq) {
    fprintf(stderr, "reused request did not advance token seq\n");
    close(managed_fd);
    return 1;
  }
  result = vyl_response_start(server, managed_token, 200,
                              (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                              (vyl_bytes){again_body, sizeof(again_body) - 1});
  if (!expect_result("duplicate response with stale seq", result,
                     VYL_STALE_TOKEN)) {
    close(managed_fd);
    return 1;
  }
  result = vyl_response_start(server, reuse_token, 200,
                              (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                              (vyl_bytes){again_body, sizeof(again_body) - 1});
  if (!expect_result("managed reuse response", result, VYL_OK)) {
    close(managed_fd);
    return 1;
  }
  if (!expect_managed_response(managed_fd, "HTTP/1.1 200 OK\r\n", "again\n", 0,
                               1)) {
    close(managed_fd);
    return 1;
  }

  int cancel_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (cancel_fd < 0) {
    perror("socket");
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(cancel_fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
      perror("connect");
      close(cancel_fd);
      return 1;
    }
  }
  const char cancel_request[] = "GET /managed HTTP/1.1\r\n"
                                "Host: localhost\r\n"
                                "\r\n";
  size_t cancel_sent = 0;
  while (cancel_sent < sizeof(cancel_request) - 1) {
    ssize_t count = send(cancel_fd, cancel_request + cancel_sent,
                         sizeof(cancel_request) - 1 - cancel_sent, 0);
    if (count <= 0) {
      perror("send");
      close(cancel_fd);
      return 1;
    }
    cancel_sent += (size_t)count;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(cancel_fd);
    return 1;
  }
  vyl_request_token cancel_token = poll_events[0].token;
  result = vyl_request_cancel(server, cancel_token);
  if (!expect_result("cancel parked request", result, VYL_OK)) {
    close(cancel_fd);
    return 1;
  }
  {
    char discard[64];
    ssize_t count = recv(cancel_fd, discard, sizeof(discard), 0);
    if (count != 0) {
      fprintf(stderr, "cancelled connection not closed, recv %zd\n", count);
      close(cancel_fd);
      return 1;
    }
  }
  close(cancel_fd);
  if (!expect_result("cancel after close", vyl_request_cancel(server, cancel_token),
                     VYL_STALE_TOKEN) ||
      !expect_result("pull after cancel",
                     vyl_request_pull_body(server, cancel_token, 1024),
                     VYL_STALE_TOKEN) ||
      !expect_result("read after cancel",
                     vyl_request_read(server, cancel_token, body_chunk,
                                      sizeof(body_chunk), &body_count),
                     VYL_STALE_TOKEN) ||
      !expect_result("respond after cancel",
                     vyl_response_start(server, cancel_token, 200,
                                        (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                                        (vyl_bytes){resp_body, sizeof(resp_body) - 1}),
                     VYL_STALE_TOKEN)) {
    return 1;
  }
  {
    vyl_request_token zero_token = {0};
    if (!expect_result("null cancel server",
                       vyl_request_cancel(NULL, cancel_token),
                       VYL_INVALID_ARGUMENT) ||
        !expect_result("zero cancel token",
                       vyl_request_cancel(server, zero_token),
                       VYL_INVALID_ARGUMENT)) {
      return 1;
    }
  }

  int cancel_upload_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (cancel_upload_fd < 0) {
    perror("socket");
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(cancel_upload_fd, (struct sockaddr *)&address, sizeof(address)) !=
        0) {
      perror("connect");
      close(cancel_upload_fd);
      return 1;
    }
  }
  const char cancel_upload_head[] = "POST /upload HTTP/1.1\r\n"
                                    "Host: localhost\r\n"
                                    "Content-Length: 10\r\n"
                                    "\r\n"
                                    "hello";
  cancel_sent = 0;
  while (cancel_sent < sizeof(cancel_upload_head) - 1) {
    ssize_t count = send(cancel_upload_fd, cancel_upload_head + cancel_sent,
                         sizeof(cancel_upload_head) - 1 - cancel_sent, 0);
    if (count <= 0) {
      perror("send");
      close(cancel_upload_fd);
      return 1;
    }
    cancel_sent += (size_t)count;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(cancel_upload_fd);
    return 1;
  }
  vyl_request_token cancel_upload_token = poll_events[0].token;
  result = vyl_request_pull_body(server, cancel_upload_token, 1024);
  if (!expect_result("cancel-upload body pull", result, VYL_OK)) {
    close(cancel_upload_fd);
    return 1;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(cancel_upload_fd);
    return 1;
  }
  result = vyl_request_cancel(server, cancel_upload_token);
  if (!expect_result("cancel mid-body request", result, VYL_OK)) {
    close(cancel_upload_fd);
    return 1;
  }
  {
    char discard[64];
    ssize_t count = recv(cancel_upload_fd, discard, sizeof(discard), 0);
    if (count != 0) {
      fprintf(stderr, "cancelled upload not closed, recv %zd\n", count);
      close(cancel_upload_fd);
      return 1;
    }
  }
  close(cancel_upload_fd);
  result = vyl_request_read(server, cancel_upload_token, body_chunk,
                            sizeof(body_chunk), &body_count);
  if (!expect_result("read after mid-body cancel", result, VYL_STALE_TOKEN)) {
    return 1;
  }

  int trailer_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (trailer_fd < 0) {
    perror("socket");
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(trailer_fd, (struct sockaddr *)&address, sizeof(address)) !=
        0) {
      perror("connect");
      close(trailer_fd);
      return 1;
    }
  }
  const char trailer_request[] = "POST /trailers HTTP/1.1\r\n"
                                 "Host: localhost\r\n"
                                 "Transfer-Encoding: chunked\r\n"
                                 "Connection: close\r\n"
                                 "\r\n"
                                 "5\r\n"
                                 "hello\r\n"
                                 "0\r\n"
                                 "X-Checksum: abc123\r\n"
                                 "X-Signature: s3cr3t\r\n"
                                 "\r\n";
  size_t trailer_sent = 0;
  while (trailer_sent < sizeof(trailer_request) - 1) {
    ssize_t count = send(trailer_fd, trailer_request + trailer_sent,
                         sizeof(trailer_request) - 1 - trailer_sent, 0);
    if (count <= 0) {
      perror("send");
      close(trailer_fd);
      return 1;
    }
    trailer_sent += (size_t)count;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(trailer_fd);
    return 1;
  }
  vyl_request_token trailer_token = poll_events[0].token;
  if (poll_events[0].kind != VYL_HOST_REQUEST) {
    fprintf(stderr, "unexpected trailer request event\n");
    close(trailer_fd);
    return 1;
  }

  vyl_header_ref trailer_refs[4];
  uint8_t trailer_data[64];
  vyl_trailer_info trailer_info = {0};
  result = vyl_request_copy_trailers(server, trailer_token, trailer_refs, 4,
                                     trailer_data, sizeof(trailer_data),
                                     &trailer_info);
  if (!expect_result("trailer copy before body end", result, VYL_WOULD_BLOCK)) {
    close(trailer_fd);
    return 1;
  }

  size_t trailer_body_total = 0;
  char trailer_body[16] = {0};
  int trailer_complete = 0;
  for (int pull = 0; pull < 8 && !trailer_complete; pull++) {
    result = vyl_request_pull_body(server, trailer_token, 1024);
    if (!expect_result("trailer body pull", result, VYL_OK)) {
      close(trailer_fd);
      return 1;
    }
    if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data),
                    &batch, 1, VYL_OK)) {
      close(trailer_fd);
      return 1;
    }
    if (poll_events[0].kind != VYL_HOST_BODY_AVAILABLE) {
      fprintf(stderr, "unexpected trailer body event\n");
      close(trailer_fd);
      return 1;
    }
    if (poll_events[0].body_complete) {
      trailer_complete = 1;
    }
    for (int read = 0; read < 4; read++) {
      result = vyl_request_read(server, trailer_token, body_chunk,
                                sizeof(body_chunk), &body_count);
      if (result == VYL_WOULD_BLOCK) {
        break;
      }
      if (result != VYL_OK && result != VYL_END_OF_BODY) {
        fprintf(stderr, "unexpected trailer body read %d\n", result);
        close(trailer_fd);
        return 1;
      }
      if (trailer_body_total + body_count >= sizeof(trailer_body)) {
        fprintf(stderr, "trailer body overflow\n");
        close(trailer_fd);
        return 1;
      }
      memcpy(trailer_body + trailer_body_total, body_chunk, body_count);
      trailer_body_total += body_count;
      if (result == VYL_END_OF_BODY) {
        break;
      }
    }
  }
  if (!trailer_complete) {
    fprintf(stderr, "trailer body never completed\n");
    close(trailer_fd);
    return 1;
  }
  if (trailer_body_total != 5 || memcmp(trailer_body, "hello", 5) != 0) {
    fprintf(stderr, "unexpected trailer body bytes\n");
    close(trailer_fd);
    return 1;
  }

  result = vyl_request_copy_trailers(server, trailer_token, trailer_refs, 4,
                                     trailer_data, sizeof(trailer_data),
                                     &trailer_info);
  if (!expect_result("trailer copy", result, VYL_OK) ||
      trailer_info.header_count != 2 || trailer_info.data_used != 33) {
    fprintf(stderr, "unexpected trailer copy %d/%u/%u\n", result,
            trailer_info.header_count, trailer_info.data_used);
    close(trailer_fd);
    return 1;
  }
  if (trailer_refs[0].name_length != 10 ||
      memcmp(trailer_data + trailer_refs[0].name_offset, "X-Checksum", 10) !=
          0 ||
      trailer_refs[0].value_length != 6 ||
      memcmp(trailer_data + trailer_refs[0].value_offset, "abc123", 6) != 0 ||
      trailer_refs[1].name_length != 11 ||
      memcmp(trailer_data + trailer_refs[1].name_offset, "X-Signature", 11) !=
          0 ||
      trailer_refs[1].value_length != 6 ||
      memcmp(trailer_data + trailer_refs[1].value_offset, "s3cr3t", 6) != 0) {
    fprintf(stderr, "unexpected trailer field bytes\n");
    close(trailer_fd);
    return 1;
  }

  result = vyl_request_copy_trailers(server, trailer_token, trailer_refs, 1,
                                     trailer_data, sizeof(trailer_data),
                                     &trailer_info);
  if (!expect_result("trailer copy small headers", result,
                     VYL_BUFFER_TOO_SMALL) ||
      trailer_info.headers_required != 2 || trailer_info.data_required != 33) {
    fprintf(stderr, "unexpected small-header copy %d/%u/%u\n", result,
            trailer_info.headers_required, trailer_info.data_required);
    close(trailer_fd);
    return 1;
  }

  uint8_t trailer_resp[] = "done\n";
  result = vyl_response_start(server, trailer_token, 200,
                              (vyl_bytes){resp_reason, sizeof(resp_reason) - 1},
                              (vyl_bytes){trailer_resp, sizeof(trailer_resp) - 1});
  if (!expect_result("trailer echo response", result, VYL_OK)) {
    close(trailer_fd);
    return 1;
  }
  if (!expect_managed_response(trailer_fd, "HTTP/1.1 200 OK\r\n", "done\n", 1,
                               1)) {
    close(trailer_fd);
    return 1;
  }

  int evil_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (evil_fd < 0) {
    perror("socket");
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(evil_fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
      perror("connect");
      close(evil_fd);
      return 1;
    }
  }
  const char evil_request[] = "POST /trailers HTTP/1.1\r\n"
                              "Host: localhost\r\n"
                              "Transfer-Encoding: chunked\r\n"
                              "Connection: close\r\n"
                              "\r\n"
                              "5\r\n"
                              "hello\r\n"
                              "0\r\n"
                              "X-Evil: 1\r\n"
                              "\r\n";
  size_t evil_sent = 0;
  while (evil_sent < sizeof(evil_request) - 1) {
    ssize_t count = send(evil_fd, evil_request + evil_sent,
                         sizeof(evil_request) - 1 - evil_sent, 0);
    if (count <= 0) {
      perror("send");
      close(evil_fd);
      return 1;
    }
    evil_sent += (size_t)count;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(evil_fd);
    return 1;
  }
  vyl_request_token evil_token = poll_events[0].token;
  result = vyl_request_pull_body(server, evil_token, 1024);
  if (!expect_result("evil body pull", result, VYL_OK)) {
    close(evil_fd);
    return 1;
  }
  memset(&batch, 0, sizeof(batch));
  result = vyl_server_poll_events(server, poll_events, 4, poll_data,
                                  sizeof(poll_data), 500000000, &batch);
  if (!expect_result("evil poll quiet", result, VYL_OK) ||
      batch.event_count != 0) {
    fprintf(stderr, "expected no events after disallowed trailer, got %u\n",
            batch.event_count);
    close(evil_fd);
    return 1;
  }
  {
    char discard[64];
    ssize_t count = recv(evil_fd, discard, sizeof(discard), 0);
    if (count != 0) {
      fprintf(stderr, "disallowed trailer did not close, recv %zd\n", count);
      close(evil_fd);
      return 1;
    }
  }
  close(evil_fd);
  result = vyl_request_pull_body(server, evil_token, 1024);
  if (!expect_result("pull after trailer reject", result, VYL_STALE_TOKEN)) {
    return 1;
  }

  {
    struct timespec wait_start, wait_end;
    clock_gettime(CLOCK_MONOTONIC, &wait_start);
    memset(&batch, 0, sizeof(batch));
    result = vyl_server_poll_events(server, poll_events, 4, poll_data,
                                    sizeof(poll_data), 5000000, &batch);
    clock_gettime(CLOCK_MONOTONIC, &wait_end);
    int64_t waited_ns =
        (int64_t)(wait_end.tv_sec - wait_start.tv_sec) * 1000000000ll +
        ((int64_t)wait_end.tv_nsec - (int64_t)wait_start.tv_nsec);
    if (!expect_result("host poll empty timeout", result, VYL_OK)) {
      return 1;
    }
    if (batch.event_count != 0) {
      fprintf(stderr, "expected empty timed poll, got %u\n", batch.event_count);
      return 1;
    }
    if (waited_ns < 5000000ll) {
      fprintf(stderr, "timed poll returned early after %lld ns\n",
              (long long)waited_ns);
      return 1;
    }
  }

  int wake_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (wake_fd < 0) {
    perror("socket");
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(wake_fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
      perror("connect");
      close(wake_fd);
      return 1;
    }
  }
  const char wake_request[] = "GET /managed HTTP/1.1\r\n"
                              "Host: localhost\r\n"
                              "\r\n";
  size_t wake_sent = 0;
  while (wake_sent < sizeof(wake_request) - 1) {
    ssize_t count = send(wake_fd, wake_request + wake_sent,
                         sizeof(wake_request) - 1 - wake_sent, 0);
    if (count <= 0) {
      perror("send");
      close(wake_fd);
      return 1;
    }
    wake_sent += (size_t)count;
  }
  memset(&batch, 0, sizeof(batch));
  result = vyl_server_poll_events(server, poll_events, 4, poll_data,
                                  sizeof(poll_data), 5000000000u, &batch);
  if (!expect_result("host poll wakeup", result, VYL_OK)) {
    close(wake_fd);
    return 1;
  }
  if (batch.event_count != 1 || poll_events[0].kind != VYL_HOST_REQUEST ||
      poll_events[0].method_length != 3 ||
      memcmp(poll_data + poll_events[0].method_offset, "GET", 3) != 0 ||
      poll_events[0].path_length != 8 ||
      memcmp(poll_data + poll_events[0].path_offset, "/managed", 8) != 0) {
    fprintf(stderr, "unexpected wakeup poll result\n");
    close(wake_fd);
    return 1;
  }
  close(wake_fd);

  int lat_fd = socket(AF_INET, SOCK_STREAM, 0);
  if (lat_fd < 0) {
    perror("socket");
    return 1;
  }
  {
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(lat_fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
      perror("connect");
      close(lat_fd);
      return 1;
    }
  }
  const char lat_request[] = "POST /upload HTTP/1.1\r\n"
                             "Host: localhost\r\n"
                             "Content-Length: 10\r\n"
                             "Connection: close\r\n"
                             "\r\n"
                             "helloworld";
  size_t lat_sent = 0;
  while (lat_sent < sizeof(lat_request) - 1) {
    ssize_t count = send(lat_fd, lat_request + lat_sent,
                         sizeof(lat_request) - 1 - lat_sent, 0);
    if (count <= 0) {
      perror("send");
      close(lat_fd);
      return 1;
    }
    lat_sent += (size_t)count;
  }
  if (!poll_until(server, poll_events, 4, poll_data, sizeof(poll_data), &batch,
                  1, VYL_OK)) {
    close(lat_fd);
    return 1;
  }
  vyl_request_token lat_token = poll_events[0].token;
  int64_t lat_total_ns = 0;
  for (int round = 0; round < 3; round++) {
    struct timespec lat_start, lat_end;
    clock_gettime(CLOCK_MONOTONIC, &lat_start);
    result = vyl_request_pull_body(server, lat_token, 4);
    if (!expect_result("latency pull", result, VYL_OK)) {
      close(lat_fd);
      return 1;
    }
    memset(&batch, 0, sizeof(batch));
    result = vyl_server_poll_events(server, poll_events, 4, poll_data,
                                    sizeof(poll_data), 5000000000u, &batch);
    clock_gettime(CLOCK_MONOTONIC, &lat_end);
    if (!expect_result("latency poll", result, VYL_OK) ||
        batch.event_count != 1 ||
        poll_events[0].kind != VYL_HOST_BODY_AVAILABLE) {
      fprintf(stderr, "unexpected latency poll result\n");
      close(lat_fd);
      return 1;
    }
    int64_t round_ns =
        (int64_t)(lat_end.tv_sec - lat_start.tv_sec) * 1000000000ll +
        ((int64_t)lat_end.tv_nsec - (int64_t)lat_start.tv_nsec);
    lat_total_ns += round_ns;
    fprintf(stderr, "command roundtrip %d: %lld ns\n", round,
            (long long)round_ns);
  }
  fprintf(stderr, "command roundtrip total: %lld ns\n",
          (long long)lat_total_ns);
  if (lat_total_ns >= 100000000ll) {
    fprintf(stderr, "managed commands did not wake the worker promptly\n");
    close(lat_fd);
    return 1;
  }
  result = vyl_request_read(server, lat_token, body_chunk, sizeof(body_chunk),
                            &body_count);
  if (!expect_result("latency body read", result, VYL_END_OF_BODY) ||
      body_count != 10 || memcmp(body_chunk, "helloworld", 10) != 0) {
    fprintf(stderr, "unexpected latency body: %d/%u\n", result, body_count);
    close(lat_fd);
    return 1;
  }
  close(lat_fd);

  {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
      perror("socket");
      return 1;
    }
    struct sockaddr_in address = {
        .sin_family = AF_INET,
        .sin_port = htons(bound_port),
    };
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
      perror("connect");
      close(fd);
      return 1;
    }
    const char keepalive_request[] = "GET /health HTTP/1.1\r\n"
                                     "Host: localhost\r\n"
                                     "\r\n";
    size_t sent = 0;
    while (sent < sizeof(keepalive_request) - 1) {
      ssize_t count = send(fd, keepalive_request + sent,
                           sizeof(keepalive_request) - 1 - sent, 0);
      if (count <= 0) {
        perror("send");
        close(fd);
        return 1;
      }
      sent += (size_t)count;
    }
    char keepalive_response[2048];
    size_t header_length = 0;
    while (header_length < sizeof(keepalive_response) - 1) {
      ssize_t count = recv(fd, keepalive_response + header_length, 1, 0);
      if (count <= 0) {
        perror("recv");
        close(fd);
        return 1;
      }
      header_length += (size_t)count;
      if (header_length >= 4 &&
          memcmp(keepalive_response + header_length - 4, "\r\n\r\n", 4) == 0) {
        break;
      }
    }
    keepalive_response[header_length] = '\0';
    if (strstr(keepalive_response, "HTTP/1.1 200 OK\r\n") == NULL) {
      fprintf(stderr, "unexpected keep-alive response:\n%s\n",
              keepalive_response);
      close(fd);
      return 1;
    }
    result = vyl_server_stop_accepting(server);
    if (!expect_result("stop accepting", result, VYL_OK)) {
      close(fd);
      return 1;
    }
    const char guard_request[] = "GET /join-guard HTTP/1.1\r\n"
                                 "Host: localhost\r\n"
                                 "Connection: close\r\n"
                                 "\r\n";
    sent = 0;
    while (sent < sizeof(guard_request) - 1) {
      ssize_t count =
          send(fd, guard_request + sent, sizeof(guard_request) - 1 - sent, 0);
      if (count <= 0) {
        perror("send");
        close(fd);
        return 1;
      }
      sent += (size_t)count;
    }
    char guard_response[2048];
    size_t received = 0;
    while (received < sizeof(guard_response) - 1) {
      ssize_t count = recv(fd, guard_response + received,
                           sizeof(guard_response) - 1 - received, 0);
      if (count == 0) {
        break;
      }
      if (count < 0) {
        perror("recv");
        close(fd);
        return 1;
      }
      received += (size_t)count;
    }
    close(fd);
    guard_response[received] = '\0';
    if (strstr(guard_response, "HTTP/1.1 200 OK\r\n") == NULL ||
        strstr(guard_response, "guarded\n") == NULL) {
      fprintf(stderr, "unexpected guard response:\n%s\n", guard_response);
      return 1;
    }
  }
  if (!expect_result("join from handler", join_guard_result, VYL_BUSY)) {
    return 1;
  }

  {
    vyl_host_event shutdown_events[4];
    uint8_t shutdown_data[256];
    vyl_batch_info shutdown_batch = {0};
    uint32_t shutdown_seen = 0;
    struct timespec shutdown_start;
    clock_gettime(CLOCK_MONOTONIC, &shutdown_start);
    for (;;) {
      memset(&shutdown_batch, 0, sizeof(shutdown_batch));
      result = vyl_server_poll_events(server, shutdown_events, 4,
                                      shutdown_data, sizeof(shutdown_data),
                                      100000000u, &shutdown_batch);
      if (!expect_result("shutdown drain poll", result, VYL_OK)) {
        return 1;
      }
      for (uint32_t i = 0; i < shutdown_batch.event_count; i++) {
        if (shutdown_events[i].kind > VYL_HOST_BODY_AVAILABLE) {
          fprintf(stderr, "unexpected shutdown event kind %u\n",
                  shutdown_events[i].kind);
          return 1;
        }
        if (shutdown_events[i].kind == VYL_HOST_SHUTDOWN) {
          shutdown_seen++;
        }
      }
      if (shutdown_seen >= 2) {
        break;
      }
      struct timespec shutdown_now;
      clock_gettime(CLOCK_MONOTONIC, &shutdown_now);
      int64_t shutdown_elapsed =
          (int64_t)(shutdown_now.tv_sec - shutdown_start.tv_sec) * 1000000000ll +
          ((int64_t)shutdown_now.tv_nsec - (int64_t)shutdown_start.tv_nsec);
      if (shutdown_elapsed > 5000000000ll) {
        fprintf(stderr, "timed out waiting for shutdown events, got %u\n",
                shutdown_seen);
        return 1;
      }
    }
    if (shutdown_seen != 2) {
      fprintf(stderr, "expected 2 shutdown events, got %u\n", shutdown_seen);
      return 1;
    }
  }

  result = vyl_server_join(server);
  if (!expect_result("join", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_destroy(server);
  if (!expect_result("destroy", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_create(&config, &server);
  if (!expect_result("duplicate server create", result, VYL_OK)) {
    return 1;
  }

  method_bytes = (vyl_bytes){(const uint8_t *)"GET", 3};
  path_bytes = (vyl_bytes){(const uint8_t *)"/health", 7};
  reason_bytes = (vyl_bytes){(const uint8_t *)"OK", 2};
  body_bytes = (vyl_bytes){NULL, 0};
  result = vyl_server_route_static(server, method_bytes, path_bytes, 200,
                                   reason_bytes, body_bytes);
  if (!expect_result("first duplicate route", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_route_static(server, method_bytes, path_bytes, 200,
                                   reason_bytes, body_bytes);
  if (!expect_result("second duplicate route", result, VYL_OK)) {
    return 1;
  }

  result = vyl_server_listen(server, loopback, 0, &bound_port, &system_error);
  if (!expect_result("freeze duplicate routes", result, VYL_DUPLICATE_ROUTE)) {
    return 1;
  }

  result = vyl_server_destroy(server);
  if (!expect_result("destroy duplicate server", result, VYL_OK)) {
    return 1;
  }

  return 0;
}
