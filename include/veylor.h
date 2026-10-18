#ifndef VEYLOR_H
#define VEYLOR_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define VEYLOR_ABI_VERSION 1u
#define VEYL_MAX_WORKERS 256u

typedef struct vyl_server vyl_server;

typedef enum {
  VYL_OK = 0,
  VYL_INVALID_ARGUMENT,
  VYL_BAD_STATE,
  VYL_CAPACITY,
  VYL_SYSTEM_ERROR,
  VYL_OUT_OF_MEMORY,
  VYL_DUPLICATE_ROUTE,
  VYL_WORKER_ERROR,
  VYL_BUSY,
  VYL_BUFFER_TOO_SMALL,
  VYL_WOULD_BLOCK,
  VYL_STALE_TOKEN,
  VYL_END_OF_BODY,
} vyl_result;

typedef struct {
  uint32_t abi_version;
  uint32_t struct_size;
  uint32_t worker_count;
  uint32_t reserved;
  uint64_t shutdown_grace_ns;
} vyl_config;

typedef struct {
  const uint8_t *data;
  uint32_t length;
} vyl_bytes;

typedef struct {
  vyl_bytes method;
  vyl_bytes path;
} vyl_native_request;

typedef struct {
  int32_t status;
  vyl_bytes reason;
  vyl_bytes body;
} vyl_native_response;

typedef int32_t (*vyl_native_handler)(void *user_data,
                                      const vyl_native_request *request,
                                      vyl_native_response *response);

int32_t vyl_server_create(const vyl_config *config, vyl_server **out_server);
int32_t vyl_server_route_static(vyl_server *server, vyl_bytes method,
                                vyl_bytes path, int32_t status,
                                vyl_bytes reason, vyl_bytes body);
 
int32_t vyl_server_route_native(vyl_server *server, vyl_bytes method,
                                vyl_bytes path, vyl_native_handler handler,
                                void *user_data);
int32_t vyl_server_route_managed(vyl_server *server, vyl_bytes method,
                                 vyl_bytes path);

typedef enum {
  VYL_HOST_REQUEST = 0,
   
  VYL_HOST_SHUTDOWN = 1,
  VYL_HOST_BODY_AVAILABLE = 2
} vyl_host_event_kind;

typedef struct {
  uint32_t worker_id;
  uint32_t slot;
  uint32_t request_seq;
  uint64_t worker_generation;
  uint64_t connection_generation;
} vyl_request_token;

typedef struct {
  uint32_t kind;
  uint32_t reserved;
  vyl_request_token token;
  uint64_t route_id;
  uint32_t method_offset, method_length;
  uint32_t path_offset, path_length;
  uint32_t body_ready, body_complete;
} vyl_host_event;

typedef struct {
  uint32_t event_count;
  uint32_t data_used;
  uint32_t events_required;
  uint32_t data_required;
} vyl_batch_info;

typedef struct {
  uint32_t name_offset;
  uint32_t name_length;
  uint32_t value_offset;
  uint32_t value_length;
} vyl_header_ref;

typedef struct {
  uint32_t header_count;
  uint32_t data_used;
  uint32_t headers_required;
  uint32_t data_required;
} vyl_trailer_info;

 
int32_t vyl_server_poll_events(vyl_server *server, vyl_host_event *events,
                               uint32_t event_capacity, uint8_t *data,
                               uint32_t data_capacity, uint64_t timeout_ns,
                               vyl_batch_info *out_info);
 
int32_t vyl_request_pull_body(vyl_server *server, vyl_request_token token,
                              uint32_t max_bytes);
 
int32_t vyl_request_read(vyl_server *server, vyl_request_token token,
                         uint8_t *dst, uint32_t capacity,
                         uint32_t *read_bytes);
 
int32_t vyl_response_start(vyl_server *server, vyl_request_token token,
                           int32_t status, vyl_bytes reason, vyl_bytes body);
 
int32_t vyl_request_cancel(vyl_server *server, vyl_request_token token);
 
int32_t vyl_server_route_managed_with_trailers(vyl_server *server,
                                               vyl_bytes method, vyl_bytes path,
                                               const vyl_bytes *trailers,
                                               uint32_t trailer_count);
 
int32_t vyl_request_copy_trailers(vyl_server *server, vyl_request_token token,
                                  vyl_header_ref *headers, uint32_t header_cap,
                                  uint8_t *data, uint32_t data_cap,
                                  vyl_trailer_info *out_info);
int32_t vyl_server_listen(vyl_server *server, const uint8_t address[4],
                          uint16_t port, uint16_t *bound_port,
                          int32_t *system_error);
int32_t vyl_server_start(vyl_server *server);
 
int32_t vyl_server_stop_accepting(vyl_server *server);
int32_t vyl_server_join(vyl_server *server);
int32_t vyl_server_destroy(vyl_server *server);

#ifdef __cplusplus
}
#endif

#endif
