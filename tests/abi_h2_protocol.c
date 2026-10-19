#define _POSIX_C_SOURCE 200809L

#include "veylor.h"
#include "abi_static_server_test.h"

#include <stdio.h>
#include <sys/socket.h>
#include <unistd.h>

int h2_send_all(int fd, const uint8_t *data, size_t length) {
  size_t sent = 0;
  while (sent < length) {
    ssize_t count = send(fd, data + sent, length - sent, 0);
    if (count <= 0) {
      return 0;
    }
    sent += (size_t)count;
  }
  return 1;
}

int h2_recv_exact(int fd, uint8_t *data, size_t length) {
  size_t received = 0;
  while (received < length) {
    ssize_t count = recv(fd, data + received, length - received, 0);
    if (count <= 0) {
      return 0;
    }
    received += (size_t)count;
  }
  return 1;
}

int h2_read_frame(int fd, uint8_t *payload, size_t cap, uint8_t *kind,
                         uint8_t *flags, uint32_t *stream, uint32_t *length) {
  uint8_t header[9];
  if (!h2_recv_exact(fd, header, sizeof(header))) {
    fprintf(stderr, "h2 frame header short read\n");
    return 0;
  }
  uint32_t body_length =
      ((uint32_t)header[0] << 16) | ((uint32_t)header[1] << 8) | header[2];
  if (body_length > cap) {
    fprintf(stderr, "h2 frame too large: %u\n", body_length);
    return 0;
  }
  *kind = header[3];
  *flags = header[4];
  *stream = ((uint32_t)(header[5] & 0x7f) << 24) | ((uint32_t)header[6] << 16) |
            ((uint32_t)header[7] << 8) | header[8];
  *length = body_length;
  return body_length == 0 || h2_recv_exact(fd, payload, body_length);
}

int h2_has_setting(const uint8_t *payload, uint32_t length, uint16_t id,
                          uint32_t value) {
  if (length % 6 != 0) {
    return 0;
  }
  for (uint32_t i = 0; i + 6 <= length; i += 6) {
    uint16_t entry_id = ((uint16_t)payload[i] << 8) | payload[i + 1];
    uint32_t entry_value = ((uint32_t)payload[i + 2] << 24) |
                           ((uint32_t)payload[i + 3] << 16) |
                           ((uint32_t)payload[i + 4] << 8) | payload[i + 5];
    if (entry_id == id && entry_value == value) {
      return 1;
    }
  }
  return 0;
}
