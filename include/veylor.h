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

int32_t vyl_server_create(const vyl_config *config, vyl_server **out_server);
int32_t vyl_server_route_static(vyl_server *server, vyl_bytes method,
                                vyl_bytes path, int32_t status,
                                vyl_bytes reason, vyl_bytes body);
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
