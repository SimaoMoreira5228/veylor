#define _POSIX_C_SOURCE 200809L

#include "veylor.h"
#include "abi_static_server_test.h"

#include <arpa/inet.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

int check_h2_worker_integration(const vyl_config *config,
                                       const uint8_t loopback[4]) {
  vyl_server *server = NULL;
  int32_t result = vyl_server_create(config, &server);
  if (!expect_result("h2 server create", result, VYL_OK)) {
    return 0;
  }

  uint16_t h2_port = 0;
  int32_t system_error = 0;
  result = vyl_server_listen_h2(server, loopback, 0, &h2_port, &system_error);
  if (!expect_result("h2 listen before h1", result, VYL_BAD_STATE)) {
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_server_listen_h2(NULL, loopback, 0, &h2_port, &system_error);
  if (!expect_result("h2 listen null server", result, VYL_INVALID_ARGUMENT)) {
    vyl_server_destroy(server);
    return 0;
  }

  vyl_bytes method = {(const uint8_t *)"GET", 3};
  vyl_bytes path = {(const uint8_t *)"/health", 7};
  vyl_bytes reason = {(const uint8_t *)"OK", 2};
  vyl_bytes body = {(const uint8_t *)"C host says hello\n", 18};
  result = vyl_server_route_static(server, method, path, 200, reason, body);
  if (!expect_result("h2 static route", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }

  uint8_t managed_method[] = "GET";
  uint8_t managed_path[] = "/managed";
  result = vyl_server_route_managed(
      server, (vyl_bytes){managed_method, sizeof(managed_method) - 1},
      (vyl_bytes){managed_path, sizeof(managed_path) - 1});
  if (!expect_result("h2 managed route", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }

  uint8_t upload_method[] = "POST";
  uint8_t upload_path[] = "/upload";
  result = vyl_server_route_managed(
      server, (vyl_bytes){upload_method, sizeof(upload_method) - 1},
      (vyl_bytes){upload_path, sizeof(upload_path) - 1});
  if (!expect_result("h2 managed upload route", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }

  uint8_t upload_trailer_method[] = "POST";
  uint8_t upload_trailer_path[] = "/upload-trailer";
  uint8_t upload_trailer_allow[] = "x-checksum";
  vyl_bytes upload_trailer_names[] = {
      {upload_trailer_allow, sizeof(upload_trailer_allow) - 1}};
  result = vyl_server_route_managed_with_trailers(
      server, (vyl_bytes){upload_trailer_method, sizeof(upload_trailer_method) - 1},
      (vyl_bytes){upload_trailer_path, sizeof(upload_trailer_path) - 1},
      upload_trailer_names, 1);
  if (!expect_result("h2 managed upload trailer route", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }

  uint16_t bound_port = 0;
  result = vyl_server_listen(server, loopback, 0, &bound_port, &system_error);
  if (!expect_result("h2 h1 listen", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_server_listen_h2(server, loopback, 0, &h2_port, &system_error);
  if (!expect_result("h2 listen", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }
  if (h2_port == 0 || h2_port == bound_port) {
    fprintf(stderr, "h2 bound to unexpected port %u (h1 %u)\n", h2_port,
            bound_port);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_server_listen_h2(server, loopback, 0, &h2_port, &system_error);
  if (!expect_result("h2 double listen", result, VYL_BAD_STATE)) {
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_server_start(server);
  if (!expect_result("h2 start", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }

  int fd = socket(AF_INET, SOCK_STREAM, 0);
  if (fd < 0) {
    perror("h2 socket");
    vyl_server_destroy(server);
    return 0;
  }
  struct timeval recv_timeout = {.tv_sec = 5, .tv_usec = 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &recv_timeout, sizeof(recv_timeout));
  struct sockaddr_in address = {
      .sin_family = AF_INET,
      .sin_port = htons(h2_port),
  };
  address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  if (connect(fd, (struct sockaddr *)&address, sizeof(address)) != 0) {
    perror("h2 connect");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

  static const uint8_t preface[] = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";
  static const uint8_t empty_settings[] = {0, 0, 0, 4, 0, 0, 0, 0, 0};
  if (!h2_send_all(fd, preface, sizeof(preface) - 1) ||
      !h2_send_all(fd, empty_settings, sizeof(empty_settings))) {
    fprintf(stderr, "h2 preface send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

  uint8_t payload[H2_TEST_PAYLOAD_CAPACITY];
  uint8_t kind = 0, flags = 0;
  uint32_t stream = 0, length = 0;
  if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                     &length) ||
      kind != 4 || stream != 0 || length != 36) {
    fprintf(stderr, "h2 expected server SETTINGS, got kind %u len %u\n", kind,
            length);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (!h2_has_setting(payload, length, 3, 32)) {
    fprintf(stderr, "h2 server SETTINGS lack max-concurrent-streams 32\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                     &length) ||
      kind != 4 || stream != 0 || length != 0 || flags != 1) {
    fprintf(stderr, "h2 expected SETTINGS ack, got kind %u flags %u\n", kind,
            flags);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

  static const uint8_t ping[] = {0, 0, 8, 6, 0, 0, 0, 0, 0,
                                 1, 2, 3, 4, 5, 6, 7, 8};
  if (!h2_send_all(fd, ping, sizeof(ping))) {
    fprintf(stderr, "h2 ping send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                     &length) ||
      kind != 6 || stream != 0 || length != 8 || flags != 1 ||
      memcmp(payload, ping + 9, 8) != 0) {
    fprintf(stderr, "h2 expected PING ack, got kind %u flags %u\n", kind,
            flags);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

  static const uint8_t headers[] = {
      0,    0,    17,   1,   5,   0,   0,   0,   1,   0x82, 0x86, 0x84,
      0x01, 0x0c, 'e',  'x', 'a', 'm', 'p', 'l', 'e', '.',  't',  'e',
      's',  't'};
  if (!h2_send_all(fd, headers, sizeof(headers))) {
    fprintf(stderr, "h2 HEADERS send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                     &length) ||
      kind != 3 || stream != 1 || length != 4 || payload[0] != 0 ||
      payload[1] != 0 || payload[2] != 0 || payload[3] != 7) {
    fprintf(stderr, "h2 expected RST_STREAM refused, got kind %u stream %u\n",
            kind, stream);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

  uint8_t h2_head[9] = {0, 0, 0, 1, 5, 0, 0, 0, 0};

   
  h2_head[2] = (uint8_t)sizeof(h2_get_managed);
  h2_head[4] = 0x05;
  h2_head[8] = 3;
  if (!h2_send_all(fd, h2_head, sizeof(h2_head)) ||
      !h2_send_all(fd, h2_get_managed, sizeof(h2_get_managed))) {
    fprintf(stderr, "h2 managed HEADERS send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_host_event h2_events[4];
  uint8_t h2_data[2048];
  vyl_batch_info h2_batch = {0};
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (h2_events[0].kind != VYL_HOST_REQUEST ||
      h2_events[0].token.stream_id != 3 ||
      h2_events[0].method_length != 3 ||
      memcmp(h2_data + h2_events[0].method_offset, "GET", 3) != 0 ||
      h2_events[0].path_length != 8 ||
      memcmp(h2_data + h2_events[0].path_offset, "/managed", 8) != 0) {
    fprintf(stderr, "h2 unexpected managed request event\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_request_token h2_token3 = h2_events[0].token;
  result = vyl_response_start(server, h2_token3, 200,
                              (vyl_bytes){h2_reason, sizeof(h2_reason) - 1},
                              (vyl_bytes){h2_body, sizeof(h2_body) - 1});
  if (!expect_result("h2 response start", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  uint32_t h2_skipped = 0;
  do {
    if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                       &length)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 8) {
      h2_skipped++;
      continue;
    }
    break;
  } while (h2_skipped < 8);
  if (kind != 1 || stream != 3 || length != 13 || (flags & 0x04) == 0 ||
      (flags & 0x01) != 0 || payload[0] != 0x00 || payload[1] != 0x07 ||
      memcmp(payload + 2, ":status", 7) != 0 || payload[9] != 0x03 ||
      memcmp(payload + 10, "200", 3) != 0) {
    fprintf(stderr, "h2 unexpected response HEADERS kind %u stream %u\n", kind,
            stream);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  h2_skipped = 0;
  do {
    if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                       &length)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 8) {
      h2_skipped++;
      continue;
    }
    break;
  } while (h2_skipped < 8);
  if (kind != 0 || stream != 3 || length != sizeof(h2_body) - 1 ||
      (flags & 0x01) == 0 ||
      memcmp(payload, h2_body, sizeof(h2_body) - 1) != 0) {
    fprintf(stderr, "h2 unexpected response DATA kind %u stream %u len %u\n",
            kind, stream, length);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  uint32_t h2_read_count = 0;
  result = vyl_request_read(server, h2_token3, h2_data, sizeof(h2_data),
                            &h2_read_count);
  if (!expect_result("h2 read after complete", result, VYL_STALE_TOKEN)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

   
  h2_head[2] = (uint8_t)sizeof(h2_post_upload);
  h2_head[4] = 0x04;
  h2_head[8] = 5;
  if (!h2_send_all(fd, h2_head, sizeof(h2_head)) ||
      !h2_send_all(fd, h2_post_upload, sizeof(h2_post_upload))) {
    fprintf(stderr, "h2 upload HEADERS send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (h2_events[0].kind != VYL_HOST_REQUEST ||
      h2_events[0].token.stream_id != 5) {
    fprintf(stderr, "h2 unexpected upload request event\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_request_token h2_token5 = h2_events[0].token;
  static const uint8_t h2_chunk0[] = {0, 0, 5, 0, 0, 0, 0, 0, 5,
                                      'h', 'e', 'l', 'l', 'o'};
  if (!h2_send_all(fd, h2_chunk0, sizeof(h2_chunk0))) {
    fprintf(stderr, "h2 DATA send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_request_pull_body(server, h2_token5, 1024);
  if (!expect_result("h2 body pull", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (h2_events[0].kind != VYL_HOST_BODY_AVAILABLE ||
      h2_events[0].token.stream_id != 5 || h2_events[0].body_ready != 5 ||
      h2_events[0].body_complete != 0) {
    fprintf(stderr, "h2 unexpected body event ready %u complete %u\n",
            h2_events[0].body_ready, h2_events[0].body_complete);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  uint8_t h2_body_buf[64];
  result = vyl_request_read(server, h2_token5, h2_body_buf, sizeof(h2_body_buf),
                            &h2_read_count);
  if (!expect_result("h2 body read", result, VYL_OK) || h2_read_count != 5 ||
      memcmp(h2_body_buf, "hello", 5) != 0) {
    fprintf(stderr, "h2 body read got %u\n", h2_read_count);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  uint32_t h2_grants = 0;
  for (int i = 0; i < 2; i++) {
    if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                       &length) ||
        kind != 8 || length != 4) {
      fprintf(stderr, "h2 expected WINDOW_UPDATE, got kind %u\n", kind);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    uint32_t increment = ((uint32_t)payload[0] << 24) |
                         ((uint32_t)payload[1] << 16) |
                         ((uint32_t)payload[2] << 8) | payload[3];
    if (increment != 5) {
      fprintf(stderr, "h2 unexpected grant increment %u\n", increment);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    h2_grants++;
  }
  if (h2_grants != 2) {
    fprintf(stderr, "h2 expected 2 grants, got %u\n", h2_grants);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static const uint8_t h2_chunk1[] = {0, 0, 3, 0, 1, 0, 0, 0, 5,
                                      'b', 'y', 'e'};
  if (!h2_send_all(fd, h2_chunk1, sizeof(h2_chunk1))) {
    fprintf(stderr, "h2 final DATA send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_request_pull_body(server, h2_token5, 1024);
  if (!expect_result("h2 body pull terminal", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (h2_events[0].kind != VYL_HOST_BODY_AVAILABLE ||
      h2_events[0].body_ready != 3 || h2_events[0].body_complete != 1) {
    fprintf(stderr, "h2 unexpected terminal body event\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_request_read(server, h2_token5, h2_body_buf, sizeof(h2_body_buf),
                            &h2_read_count);
  if (!expect_result("h2 terminal body read", result, VYL_END_OF_BODY) ||
      h2_read_count != 3 || memcmp(h2_body_buf, "bye", 3) != 0) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static uint8_t h2_echo[] = "bye";
  result = vyl_response_start(server, h2_token5, 200,
                              (vyl_bytes){h2_reason, sizeof(h2_reason) - 1},
                              (vyl_bytes){h2_echo, sizeof(h2_echo) - 1});
  if (!expect_result("h2 echo response", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  h2_skipped = 0;
  do {
    if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                       &length)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 8) {
      h2_skipped++;
      continue;
    }
    break;
  } while (h2_skipped < 8);
  if (kind != 1 || stream != 5) {
    fprintf(stderr, "h2 expected echo HEADERS, got kind %u stream %u\n", kind,
            stream);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  h2_skipped = 0;
  do {
    if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                       &length)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 8) {
      h2_skipped++;
      continue;
    }
    break;
  } while (h2_skipped < 8);
  if (kind != 0 || stream != 5 || length != 3 ||
      memcmp(payload, "bye", 3) != 0) {
    fprintf(stderr, "h2 expected echo DATA, got kind %u len %u\n", kind,
            length);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

  if (!check_h2_followup_streams(server, fd, payload, h2_head)) {
    return 0;
  }

  result = vyl_server_stop_accepting(server);
  if (!expect_result("h2 stop accepting", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  if (!h2_read_frame(fd, payload, sizeof(payload), &kind, &flags, &stream,
                     &length) ||
      kind != 7 || stream != 0 || length != 8 || payload[4] != 0 ||
      payload[5] != 0 || payload[6] != 0 || payload[7] != 0) {
    fprintf(stderr, "h2 expected GOAWAY no-error, got kind %u\n", kind);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  close(fd);

  vyl_host_event shutdown_events[4];
  uint8_t shutdown_data[256];
  vyl_batch_info shutdown_batch = {0};
  uint32_t shutdown_seen = 0;
  for (int i = 0; i < 200 && shutdown_seen < 2; i++) {
    memset(&shutdown_batch, 0, sizeof(shutdown_batch));
    result = vyl_server_poll_events(server, shutdown_events, 4, shutdown_data,
                                    sizeof(shutdown_data), 100000000u,
                                    &shutdown_batch);
    if (!expect_result("h2 shutdown drain poll", result, VYL_OK)) {
      vyl_server_destroy(server);
      return 0;
    }
    for (uint32_t e = 0; e < shutdown_batch.event_count; e++) {
      if (shutdown_events[e].kind == VYL_HOST_SHUTDOWN) {
        shutdown_seen++;
      }
    }
  }
  if (shutdown_seen != 2) {
    fprintf(stderr, "h2 expected 2 shutdown events, got %u\n", shutdown_seen);
    vyl_server_destroy(server);
    return 0;
  }

  result = vyl_server_join(server);
  if (!expect_result("h2 join", result, VYL_OK)) {
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_server_destroy(server);
  if (!expect_result("h2 destroy", result, VYL_OK)) {
    return 0;
  }
  return 1;
}
