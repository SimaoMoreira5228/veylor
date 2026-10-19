#ifndef ABI_STATIC_SERVER_TEST_H
#define ABI_STATIC_SERVER_TEST_H

#include "veylor.h"

#include <stddef.h>
#include <stdint.h>

#define H2_TEST_PAYLOAD_CAPACITY 256

int poll_until(vyl_server *server, vyl_host_event *events,
               uint32_t event_capacity, uint8_t *data, uint32_t data_capacity,
               vyl_batch_info *batch, uint32_t want_events,
               int32_t want_result);
int expect_result(const char *operation, int32_t actual, int32_t expected);
int check_h2_worker_integration(const vyl_config *config,
                                const uint8_t loopback[4]);
int h2_send_all(int fd, const uint8_t *data, size_t length);
int h2_recv_exact(int fd, uint8_t *data, size_t length);
int h2_read_frame(int fd, uint8_t *payload, size_t cap, uint8_t *kind,
                  uint8_t *flags, uint32_t *stream, uint32_t *length);
int h2_has_setting(const uint8_t *payload, uint32_t length, uint16_t id,
                   uint32_t value);
int check_h2_followup_streams(vyl_server *server, int fd,
                              uint8_t *payload, uint8_t h2_head[9]);

static const uint8_t h2_get_managed[] = {
    0x82, 0x86, 0x04, 0x08, '/',  'm', 'a', 'n', 'a', 'g', 'e', 'd',
    0x01, 0x0c, 'e',  'x',  'a',  'm', 'p', 'l', 'e', '.', 't',  'e',
    's',  't'};
static const uint8_t h2_post_upload[] = {
    0x02, 0x04, 'P',  'O',  'S',  'T',  0x86, 0x04, 0x07, '/',
    'u',  'p',  'l',  'o',  'a',  'd',  0x01, 0x0c, 'e',  'x',
    'a',  'm',  'p',  'l',  'e',  '.',  't',  'e',  's',  't'};
static const uint8_t h2_reason[] = "OK";
static const uint8_t h2_body[] = "hello\n";

#endif
