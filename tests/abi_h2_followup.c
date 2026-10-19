#define _POSIX_C_SOURCE 200809L

#include "veylor.h"
#include "abi_static_server_test.h"

#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

int check_h2_followup_streams(vyl_server *server, int fd,
                                     uint8_t *payload,
                                     uint8_t h2_head[9]) {
  uint8_t kind = 0;
  uint8_t flags = 0;
  uint32_t stream = 0;
  uint32_t length = 0;
  uint32_t h2_skipped = 0;
  int32_t result = VYL_OK;
  vyl_host_event h2_events[4];
  uint8_t h2_data[2048];
  vyl_batch_info h2_batch = {0};

   
  h2_head[2] = (uint8_t)sizeof(h2_get_managed);
  h2_head[4] = 0x05;
  h2_head[8] = 7;
  if (!h2_send_all(fd, h2_head, 9) ||
      !h2_send_all(fd, h2_get_managed, sizeof(h2_get_managed))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  h2_head[8] = 9;
  if (!h2_send_all(fd, h2_head, 9) ||
      !h2_send_all(fd, h2_get_managed, sizeof(h2_get_managed))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  uint32_t h2_seen = 0;
  vyl_request_token h2_mux[2];
  uint32_t h2_mux_count = 0;
  for (int i = 0; i < 10 && h2_seen != 0x3u; i++) {
    memset(&h2_batch, 0, sizeof(h2_batch));
    result = vyl_server_poll_events(server, h2_events, 4, h2_data,
                                    sizeof(h2_data), 100000000u, &h2_batch);
    if (!expect_result("h2 multiplex poll", result, VYL_OK)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    for (uint32_t e = 0; e < h2_batch.event_count; e++) {
      if (h2_events[e].kind != VYL_HOST_REQUEST) {
        fprintf(stderr, "h2 unexpected multiplex event %u\n",
                h2_events[e].kind);
        close(fd);
        vyl_server_destroy(server);
        return 0;
      }
      if (h2_events[e].token.stream_id == 7) {
        h2_seen |= 0x1u;
      }
      if (h2_events[e].token.stream_id == 9) {
        h2_seen |= 0x2u;
      }
      if (h2_mux_count < 2) {
        h2_mux[h2_mux_count++] = h2_events[e].token;
      }
    }
  }
  if (h2_seen != 0x3u || h2_mux_count != 2) {
    fprintf(stderr, "h2 multiplex missing streams, saw %u\n", h2_seen);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static uint8_t h2_mux_body[] = "mux\n";
  for (uint32_t m = 0; m < 2; m++) {
    result = vyl_response_start(server, h2_mux[m], 200,
                                (vyl_bytes){h2_reason, sizeof(h2_reason) - 1},
                                (vyl_bytes){h2_mux_body, sizeof(h2_mux_body) - 1});
    if (!expect_result("h2 multiplex response", result, VYL_OK)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    h2_skipped = 0;
    do {
      if (!h2_read_frame(fd, payload, H2_TEST_PAYLOAD_CAPACITY, &kind, &flags, &stream,
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
    if (kind != 1 || stream != h2_mux[m].stream_id) {
      fprintf(stderr, "h2 multiplex HEADERS on wrong stream %u\n", stream);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    h2_skipped = 0;
    do {
      if (!h2_read_frame(fd, payload, H2_TEST_PAYLOAD_CAPACITY, &kind, &flags, &stream,
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
    if (kind != 0 || stream != h2_mux[m].stream_id || length != 4 ||
        memcmp(payload, "mux\n", 4) != 0) {
      fprintf(stderr, "h2 multiplex DATA on wrong stream %u\n", stream);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
  }

   
  h2_head[2] = (uint8_t)sizeof(h2_get_managed);
  h2_head[4] = 0x04;
  h2_head[8] = 11;
  if (!h2_send_all(fd, h2_head, 9) ||
      !h2_send_all(fd, h2_get_managed, sizeof(h2_get_managed))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].token.stream_id != 11) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_request_token h2_token11 = h2_events[0].token;
  result = vyl_response_begin(server, h2_token11, 200,
                              (vyl_bytes){h2_reason, sizeof(h2_reason) - 1});
  if (!expect_result("h2 stream begin", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].kind != VYL_HOST_WRITABLE) {
    fprintf(stderr, "h2 expected writable after begin\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static uint8_t h2_c0[] = "chunk-one";
  static uint8_t h2_c1[] = "chunk-two";
  result = vyl_response_write(server, h2_token11,
                              (vyl_bytes){h2_c0, sizeof(h2_c0) - 1});
  if (!expect_result("h2 stream write one", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_response_write(server, h2_token11,
                              (vyl_bytes){h2_c1, sizeof(h2_c1) - 1});
  if (!expect_result("h2 stream write two", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_response_end(server, h2_token11);
  if (!expect_result("h2 stream end", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  uint32_t h2_got_chunks = 0;
  uint32_t h2_got_end = 0;
  for (int i = 0; i < 6; i++) {
    h2_skipped = 0;
    do {
      if (!h2_read_frame(fd, payload, H2_TEST_PAYLOAD_CAPACITY, &kind, &flags, &stream,
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
    if (stream != 11) {
      fprintf(stderr, "h2 stream frame on wrong stream %u\n", stream);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 1) {
      continue;
    }
    if (kind != 0) {
      fprintf(stderr, "h2 unexpected stream frame kind %u\n", kind);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (length == 9 && memcmp(payload, "chunk-one", 9) == 0) {
      h2_got_chunks |= 0x1u;
    }
    if (length == 9 && memcmp(payload, "chunk-two", 9) == 0) {
      h2_got_chunks |= 0x2u;
    }
    if (length == 0 && (flags & 0x01) != 0) {
      h2_got_end = 1;
      break;
    }
  }
  if (h2_got_chunks != 0x3u || !h2_got_end) {
    fprintf(stderr, "h2 chunks %u end %u\n", h2_got_chunks, h2_got_end);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static const uint8_t h2_rst11[] = {0, 0, 4, 3, 0, 0, 0, 0, 11,
                                     0, 0, 0, 8};
  if (!h2_send_all(fd, h2_rst11, sizeof(h2_rst11))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

   
  h2_head[2] = (uint8_t)sizeof(h2_post_upload);
  h2_head[4] = 0x04;
  h2_head[8] = 13;
  if (!h2_send_all(fd, h2_head, 9) ||
      !h2_send_all(fd, h2_post_upload, sizeof(h2_post_upload))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].token.stream_id != 13) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_request_token h2_token13 = h2_events[0].token;
  static const uint8_t h2_peer_rst[] = {0, 0, 4, 3, 0, 0, 0, 0, 13,
                                        0, 0, 0, 8};
  if (!h2_send_all(fd, h2_peer_rst, sizeof(h2_peer_rst))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  struct timespec h2_pause = {.tv_sec = 0, .tv_nsec = 50000000};
  nanosleep(&h2_pause, NULL);
  result = vyl_request_pull_body(server, h2_token13, 1024);
  if (!expect_result("h2 pull after peer rst", result, VYL_STALE_TOKEN)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

   
  h2_head[2] = (uint8_t)sizeof(h2_post_upload);
  h2_head[4] = 0x04;
  h2_head[8] = 15;
  if (!h2_send_all(fd, h2_head, 9) ||
      !h2_send_all(fd, h2_post_upload, sizeof(h2_post_upload))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].token.stream_id != 15) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_request_cancel(server, h2_events[0].token);
  if (!expect_result("h2 host cancel", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  h2_skipped = 0;
  do {
    if (!h2_read_frame(fd, payload, H2_TEST_PAYLOAD_CAPACITY, &kind, &flags, &stream,
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
  if (kind != 3 || stream != 15 || length != 4 || payload[0] != 0 ||
      payload[1] != 0 || payload[2] != 0 || payload[3] != 8) {
    fprintf(stderr, "h2 expected RST cancel, got kind %u stream %u\n", kind,
            stream);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

   
  static const uint8_t h2_post_upload_trailer[] = {
      0x02, 0x04, 'P',  'O',  'S',  'T',  0x86, 0x04, 0x0f, '/',
      'u',  'p',  'l',  'o',  'a',  'd',  '-',  't',  'r',  'a',
      'i',  'l',  'e',  'r',  0x01, 0x0c, 'e',  'x',  'a',  'm',
      'p',  'l',  'e',  '.',  't',  'e',  's',  't'};
  h2_head[2] = (uint8_t)sizeof(h2_post_upload_trailer);
  h2_head[4] = 0x04;
  h2_head[8] = 17;
  if (!h2_send_all(fd, h2_head, 9) ||
      !h2_send_all(fd, h2_post_upload_trailer,
                    sizeof(h2_post_upload_trailer))) {
    fprintf(stderr, "h2 trailer upload HEADERS send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].kind != VYL_HOST_REQUEST ||
      h2_events[0].token.stream_id != 17) {
    fprintf(stderr, "h2 unexpected trailer upload request event\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_request_token h2_token17 = h2_events[0].token;
  result = vyl_request_pull_body(server, h2_token17, 1024);
  if (!expect_result("h2 trailer upload pull", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static const uint8_t h2_trailer_chunk[] = {0, 0, 5, 0, 0, 0, 0, 0, 17,
                                             'h', 'e', 'l', 'l', 'o'};
  if (!h2_send_all(fd, h2_trailer_chunk, sizeof(h2_trailer_chunk))) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].kind != VYL_HOST_BODY_AVAILABLE ||
      h2_events[0].body_ready != 5 || h2_events[0].body_complete != 0) {
    fprintf(stderr, "h2 unexpected trailer upload body event\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  uint32_t h2_trailer_read = 0;
  result = vyl_request_read(server, h2_token17, payload, H2_TEST_PAYLOAD_CAPACITY,
                            &h2_trailer_read);
  if (!expect_result("h2 trailer upload body read", result, VYL_OK) ||
      h2_trailer_read != 5 || memcmp(payload, "hello", 5) != 0) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static uint8_t h2_trailer_block[] = {
      0x00, 0x0a, 'x', '-', 'c', 'h', 'e', 'c', 'k', 's',
      'u',  'm',  0x03, 'a', 'b', 'c'};
  uint8_t h2_trailer_head[9] = {0, 0, sizeof(h2_trailer_block),
                                1, 0x05, 0, 0, 0, 17};
  if (!h2_send_all(fd, h2_trailer_head, sizeof(h2_trailer_head)) ||
      !h2_send_all(fd, h2_trailer_block, sizeof(h2_trailer_block))) {
    fprintf(stderr, "h2 trailer HEADERS send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].kind != VYL_HOST_BODY_AVAILABLE ||
      h2_events[0].body_ready != 0 || h2_events[0].body_complete != 1) {
    fprintf(stderr, "h2 unexpected trailer completion event\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_header_ref h2_trailer_refs[4];
  static uint8_t h2_trailer_data[64];
  vyl_trailer_info h2_trailer_info = {0};
  result = vyl_request_copy_trailers(server, h2_token17, NULL, 0, NULL, 0,
                                     &h2_trailer_info);
  if (!expect_result("h2 trailer probe too small", result,
                     VYL_BUFFER_TOO_SMALL) ||
      h2_trailer_info.headers_required != 1 ||
      h2_trailer_info.data_required != 13) {
    fprintf(stderr, "h2 unexpected trailer probe counts\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  result = vyl_request_copy_trailers(server, h2_token17, h2_trailer_refs, 4,
                                     h2_trailer_data, sizeof(h2_trailer_data),
                                     &h2_trailer_info);
  if (!expect_result("h2 trailer copy", result, VYL_OK) ||
      h2_trailer_info.header_count != 1 || h2_trailer_info.data_used != 13 ||
      h2_trailer_refs[0].name_length != 10 ||
      h2_trailer_refs[0].value_length != 3 ||
      memcmp(h2_trailer_data + h2_trailer_refs[0].name_offset, "x-checksum",
             10) != 0 ||
      memcmp(h2_trailer_data + h2_trailer_refs[0].value_offset, "abc", 3) !=
          0) {
    fprintf(stderr, "h2 unexpected trailer copy content\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static uint8_t h2_trailer_reason[] = "OK";
  static uint8_t h2_trailer_body[] = "done\n";
  result = vyl_response_start(server, h2_token17, 200,
                              (vyl_bytes){h2_trailer_reason,
                                          sizeof(h2_trailer_reason) - 1},
                              (vyl_bytes){h2_trailer_body,
                                          sizeof(h2_trailer_body) - 1});
  if (!expect_result("h2 trailer upload respond", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  h2_skipped = 0;
  int h2_trailer_done = 0;
  do {
    if (!h2_read_frame(fd, payload, H2_TEST_PAYLOAD_CAPACITY, &kind, &flags, &stream,
                       &length)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 8) {
      h2_skipped++;
      continue;
    }
    if (stream != 17) {
      fprintf(stderr, "h2 trailer frame on wrong stream %u\n", stream);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 1 && (flags & 0x04) != 0) {
      continue;
    }
    if (kind == 0 && length == 5 && memcmp(payload, "done\n", 5) == 0) {
      h2_trailer_done = 1;
      if ((flags & 0x01) != 0) {
        break;
      }
      continue;
    }
    if (kind == 0 && length == 0 && (flags & 0x01) != 0) {
      break;
    }
    fprintf(stderr, "h2 unexpected trailer upload frame kind %u len %u\n",
            kind, length);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  } while (h2_skipped < 8);
  if (!h2_trailer_done) {
    fprintf(stderr, "h2 trailer upload missing echo\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

   
  h2_head[2] = (uint8_t)sizeof(h2_get_managed);
  h2_head[4] = 0x05;
  h2_head[8] = 19;
  if (!h2_send_all(fd, h2_head, 9) ||
      !h2_send_all(fd, h2_get_managed, sizeof(h2_get_managed))) {
    fprintf(stderr, "h2 trailer response HEADERS send failed\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].kind != VYL_HOST_REQUEST ||
      h2_events[0].token.stream_id != 19) {
    fprintf(stderr, "h2 unexpected trailer response request event\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  vyl_request_token h2_token19 = h2_events[0].token;
  result = vyl_response_begin(server, h2_token19, 200,
                              (vyl_bytes){h2_reason, sizeof(h2_reason) - 1});
  if (!expect_result("h2 trailer response begin", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  memset(&h2_batch, 0, sizeof(h2_batch));
  if (!poll_until(server, h2_events, 4, h2_data, sizeof(h2_data), &h2_batch, 1,
                  VYL_OK) ||
      h2_events[0].kind != VYL_HOST_WRITABLE) {
    fprintf(stderr, "h2 expected writable before trailers\n");
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  static uint8_t h2_resp_trailer_data[] = "x-checksumabc";
  vyl_header_ref h2_resp_trailer_ref = {0, 10, 10, 3};
  result = vyl_response_trailers(server, h2_token19, &h2_resp_trailer_ref, 1,
                                 h2_resp_trailer_data,
                                 sizeof(h2_resp_trailer_data) - 1);
  if (!expect_result("h2 trailer send", result, VYL_OK)) {
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }
  h2_skipped = 0;
  int h2_trailer_head_seen = 0;
  int h2_trailer_end_seen = 0;
  do {
    if (!h2_read_frame(fd, payload, H2_TEST_PAYLOAD_CAPACITY, &kind, &flags, &stream,
                       &length)) {
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 8) {
      h2_skipped++;
      continue;
    }
    if (stream != 19) {
      fprintf(stderr, "h2 trailer response frame on wrong stream %u\n",
              stream);
      close(fd);
      vyl_server_destroy(server);
      return 0;
    }
    if (kind == 1 && (flags & 0x04) != 0 && (flags & 0x01) == 0) {
      h2_trailer_head_seen = 1;
      continue;
    }
    if (kind == 1 && (flags & 0x01) != 0 && length == sizeof(h2_trailer_block) &&
        memcmp(payload, h2_trailer_block, sizeof(h2_trailer_block)) == 0) {
      h2_trailer_end_seen = 1;
      break;
    }
    fprintf(stderr, "h2 unexpected trailer response frame kind %u len %u\n",
            kind, length);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  } while (h2_skipped < 8);
  if (!h2_trailer_head_seen || !h2_trailer_end_seen) {
    fprintf(stderr, "h2 trailer response incomplete head %d end %d\n",
            h2_trailer_head_seen, h2_trailer_end_seen);
    close(fd);
    vyl_server_destroy(server);
    return 0;
  }

  return 1;
}
