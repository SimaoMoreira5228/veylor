#include "veylor.h"

#include <arpa/inet.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

_Static_assert(sizeof(vyl_config) == 24, "vyl_config layout changed");
_Static_assert(offsetof(vyl_config, shutdown_grace_ns) == 16,
               "vyl_config offset changed");
_Static_assert(sizeof(vyl_bytes) == 16, "vyl_bytes layout changed");

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
  result = vyl_server_stop_accepting(server);
  if (!expect_result("stop accepting", result, VYL_OK)) {
    return 1;
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
