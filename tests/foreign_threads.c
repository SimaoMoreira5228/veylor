#define _GNU_SOURCE

#include "veylor_http1.h"

#include <arpa/inet.h>
#include <assert.h>
#include <errno.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

_Static_assert(sizeof(vyl_managed_static_token) == 32, "token layout must match Rust");
_Static_assert(sizeof(vyl_managed_static_event) == 80, "event layout must match Rust");
_Static_assert(sizeof(vyl_managed_static_poll_report) == 32, "report layout must match Rust");
_Static_assert(VYL_HTTP1_WOULD_BLOCK == 7, "would-block code must match Rust");
_Static_assert(VYL_HTTP1_STALE_TOKEN == 12, "stale code must match Rust");
_Static_assert(VYL_HTTP1_DUPLICATE == 13, "duplicate code must match Rust");
_Static_assert(VYL_HTTP1_GONE == 14, "gone code must match Rust");

/* NOTE: contract as found in the sources (crates/veylor-ffi/src/managed_static/,
   crates/veylor-server/src/host/mod.rs, connection/streams.rs). Every Running-state
   dispatch entry (poll_events, host_respond/cancel/pull_body/pull_trailers,
   host_begin/write/end_stream(+trailers), host_is_writable) takes only a shared
   handle ref, clones the HostHandle Arc, and serializes on the HostShared
   Mutex<HostInner> with short copy-only critical sections; catch_ffi contains
   panics per call and saturation reports WOULD_BLOCK. So concurrent dispatch
   calls on one live handle from foreign threads are legal, including concurrent
   polls (separate caller buffers each) and poll+respond+pull+is_writable mixes.
   Boundary NOT tested here: lifecycle mutation (new/register/start/join/destroy)
   takes &mut state and needs exclusive access; destroy-while-running fails
   closed and join-from-native-handler reports BUSY. Caller buffers stay
   per-thread exclusive; tokens cross threads by value through the queues below. */

#define FT_QUEUE_DEPTH 64
#define FT_ONE_SHOTS 4
#define FT_STREAMS 1
#define FT_EXCHANGES (FT_ONE_SHOTS + FT_STREAMS)
#define FT_POLLERS 2

typedef struct {
    vyl_managed_static_token token;
    char path[16];
    size_t path_len;
} ft_request;

typedef struct {
    pthread_mutex_t mutex;
    pthread_cond_t cond;
    ft_request items[FT_QUEUE_DEPTH];
    size_t head;
    size_t count;
    int done;
} ft_queue;

static atomic_uint g_observed = 0;
static atomic_uint g_responded = 0;
static atomic_uint g_begun = 0;
static atomic_uint g_written = 0;

static void ft_sane(int rc) {
    assert(
        rc == VYL_HTTP1_OK || rc == VYL_HTTP1_WOULD_BLOCK || rc == VYL_HTTP1_STALE_TOKEN ||
        rc == VYL_HTTP1_GONE || rc == VYL_HTTP1_DUPLICATE || rc == VYL_HTTP1_BAD_STATE
    );
}

static void ft_abs_deadline(struct timespec* deadline) {
    assert(clock_gettime(CLOCK_REALTIME, deadline) == 0);
    deadline->tv_sec += 10;
}

static void ft_queue_init(ft_queue* queue) {
    assert(pthread_mutex_init(&queue->mutex, NULL) == 0);
    assert(pthread_cond_init(&queue->cond, NULL) == 0);
    queue->head = 0;
    queue->count = 0;
    queue->done = 0;
}

static void ft_queue_push(ft_queue* queue, const ft_request* request) {
    assert(pthread_mutex_lock(&queue->mutex) == 0);
    while (queue->count == FT_QUEUE_DEPTH && !queue->done) {
        struct timespec deadline = {0};
        ft_abs_deadline(&deadline);
        assert(pthread_cond_timedwait(&queue->cond, &queue->mutex, &deadline) == 0);
    }
    assert(queue->count < FT_QUEUE_DEPTH);
    assert(!queue->done);
    queue->items[(queue->head + queue->count) % FT_QUEUE_DEPTH] = *request;
    queue->count++;
    assert(pthread_cond_signal(&queue->cond) == 0);
    assert(pthread_mutex_unlock(&queue->mutex) == 0);
}

static int ft_queue_pop(ft_queue* queue, ft_request* out) {
    assert(pthread_mutex_lock(&queue->mutex) == 0);
    while (queue->count == 0 && !queue->done) {
        struct timespec deadline = {0};
        ft_abs_deadline(&deadline);
        assert(pthread_cond_timedwait(&queue->cond, &queue->mutex, &deadline) == 0);
    }
    int got = 0;
    if (queue->count > 0) {
        *out = queue->items[queue->head];
        queue->head = (queue->head + 1) % FT_QUEUE_DEPTH;
        queue->count--;
        got = 1;
        assert(pthread_cond_signal(&queue->cond) == 0);
    }
    assert(pthread_mutex_unlock(&queue->mutex) == 0);
    return got;
}

static void ft_queue_finish(ft_queue* queue) {
    assert(pthread_mutex_lock(&queue->mutex) == 0);
    queue->done = 1;
    assert(pthread_cond_broadcast(&queue->cond) == 0);
    assert(pthread_mutex_unlock(&queue->mutex) == 0);
}

static int ft_queue_finished(ft_queue* queue) {
    assert(pthread_mutex_lock(&queue->mutex) == 0);
    int done = queue->done;
    assert(pthread_mutex_unlock(&queue->mutex) == 0);
    return done;
}

static void ft_barrier_wait(pthread_barrier_t* barrier) {
    int rc = pthread_barrier_wait(barrier);
    assert(rc == 0 || rc == PTHREAD_BARRIER_SERIAL_THREAD);
}

static void ft_join_timed(pthread_t thread) {
    struct timespec deadline = {0};
    assert(clock_gettime(CLOCK_REALTIME, &deadline) == 0);
    deadline.tv_sec += 20;
    assert(pthread_timedjoin_np(thread, NULL, &deadline) == 0);
}

static void ft_respond_ok(vyl_managed_static_server* server, vyl_managed_static_token token) {
    static const uint8_t reason[] = "OK";
    static const uint8_t body[] = "managed-hi";
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    for (int i = 0; i < 5000; i++) {
        int rc = vyl_managed_static_host_respond(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1},
            (vyl_http1_bytes){.data = body, .length = sizeof(body) - 1}
        );
        ft_sane(rc);
        if (rc == VYL_HTTP1_OK) {
            return;
        }
        assert(rc == VYL_HTTP1_WOULD_BLOCK);
        nanosleep(&pause, NULL);
    }
    assert(0 && "respond never admitted");
}

static void ft_begin_ok(vyl_managed_static_server* server, vyl_managed_static_token token) {
    static const uint8_t reason[] = "OK";
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    for (int i = 0; i < 5000; i++) {
        int rc = vyl_managed_static_host_begin_stream(
            server,
            token,
            200,
            (vyl_http1_bytes){.data = reason, .length = sizeof(reason) - 1}
        );
        ft_sane(rc);
        if (rc == VYL_HTTP1_OK) {
            return;
        }
        assert(rc == VYL_HTTP1_WOULD_BLOCK);
        nanosleep(&pause, NULL);
    }
    assert(0 && "begin never admitted");
}

static void ft_wait_writable(vyl_managed_static_server* server, vyl_managed_static_token token) {
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 10000000};
    for (int i = 0; i < 500; i++) {
        uint32_t writable = 0;
        assert(vyl_managed_static_host_is_writable(server, token, &writable) == VYL_HTTP1_OK);
        if (writable == 1) {
            return;
        }
        nanosleep(&pause, NULL);
    }
    assert(0 && "stream never armed writable");
}

static void ft_write_ok(
    vyl_managed_static_server* server,
    vyl_managed_static_token token,
    const uint8_t* chunk,
    size_t chunk_len
) {
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    for (int i = 0; i < 5000; i++) {
        int rc = vyl_managed_static_host_write_stream(
            server,
            token,
            (vyl_http1_bytes){.data = chunk, .length = chunk_len}
        );
        ft_sane(rc);
        if (rc == VYL_HTTP1_OK) {
            return;
        }
        assert(rc == VYL_HTTP1_WOULD_BLOCK || rc == VYL_HTTP1_BAD_STATE);
        nanosleep(&pause, NULL);
    }
    assert(0 && "write never admitted");
}

static void ft_end_ok(vyl_managed_static_server* server, vyl_managed_static_token token) {
    const struct timespec pause = {.tv_sec = 0, .tv_nsec = 1000000};
    for (int i = 0; i < 5000; i++) {
        int rc = vyl_managed_static_host_end_stream(server, token);
        ft_sane(rc);
        if (rc == VYL_HTTP1_OK) {
            return;
        }
        assert(rc == VYL_HTTP1_WOULD_BLOCK);
        nanosleep(&pause, NULL);
    }
    assert(0 && "end never admitted");
}

typedef struct {
    vyl_managed_static_server* server;
    pthread_barrier_t* start;
    ft_queue* incoming;
} ft_poller_arg;

static void* ft_poller_main(void* raw) {
    ft_poller_arg* arg = raw;
    vyl_managed_static_event events[8] = {0};
    uint8_t methods[256] = {0};
    uint8_t paths[4096] = {0};
    ft_barrier_wait(arg->start);
    for (int i = 0; i < 100000; i++) {
        vyl_managed_static_poll_report report = {0};
        assert(
            vyl_managed_static_poll_events(
                arg->server,
                100000000u,
                events,
                8,
                methods,
                sizeof(methods),
                paths,
                sizeof(paths),
                &report
            ) == VYL_HTTP1_OK
        );
        for (size_t k = 0; k < report.copied; k++) {
            if (events[k].kind != (uint32_t)VYL_MANAGED_STATIC_EVENT_REQUEST) {
                continue;
            }
            ft_request request = {0};
            request.token = events[k].token;
            assert(events[k].path_len < sizeof(request.path));
            memcpy(request.path, paths + events[k].path_offset, events[k].path_len);
            request.path_len = events[k].path_len;
            ft_queue_push(arg->incoming, &request);
            atomic_fetch_add(&g_observed, 1);
        }
        if (ft_queue_finished(arg->incoming)) {
            break;
        }
    }
    return NULL;
}

typedef struct {
    vyl_managed_static_server* server;
    pthread_barrier_t* start;
    ft_queue* incoming;
    ft_queue* streams;
} ft_worker_arg;

static void* ft_responder_main(void* raw) {
    ft_worker_arg* arg = raw;
    ft_barrier_wait(arg->start);
    for (;;) {
        ft_request request = {0};
        if (!ft_queue_pop(arg->incoming, &request)) {
            break;
        }
        if (request.path_len == 2 && memcmp(request.path, "/s", 2) == 0) {
            ft_begin_ok(arg->server, request.token);
            atomic_fetch_add(&g_begun, 1);
            ft_queue_push(arg->streams, &request);
        } else {
            assert(request.path_len == 2 && memcmp(request.path, "/m", 2) == 0);
            ft_respond_ok(arg->server, request.token);
            atomic_fetch_add(&g_responded, 1);
        }
    }
    return NULL;
}

static void* ft_writer_main(void* raw) {
    ft_worker_arg* arg = raw;
    static const uint8_t first_chunk[] = "hi";
    static const uint8_t second_chunk[] = "yo";
    ft_barrier_wait(arg->start);
    for (;;) {
        ft_request request = {0};
        if (!ft_queue_pop(arg->streams, &request)) {
            break;
        }
        ft_wait_writable(arg->server, request.token);
        ft_write_ok(arg->server, request.token, first_chunk, sizeof(first_chunk) - 1);
        ft_wait_writable(arg->server, request.token);
        ft_write_ok(arg->server, request.token, second_chunk, sizeof(second_chunk) - 1);
        ft_wait_writable(arg->server, request.token);
        ft_end_ok(arg->server, request.token);
        atomic_fetch_add(&g_written, 1);
    }
    return NULL;
}

static int connect_loopback(uint16_t port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    assert(fd >= 0);
    const struct timeval timeout = {.tv_sec = 10, .tv_usec = 0};
    assert(setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) == 0);
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    assert(inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr) == 1);
    assert(connect(fd, (const struct sockaddr*)&addr, sizeof(addr)) == 0);
    return fd;
}

static size_t read_to_end(int fd, char* out, size_t capacity) {
    size_t total = 0;
    for (;;) {
        ssize_t received = recv(fd, out + total, capacity - 1 - total, 0);
        assert(received >= 0);
        if (received == 0) {
            break;
        }
        total += (size_t)received;
        assert(total < capacity - 1);
    }
    out[total] = '\0';
    return total;
}

int main(void) {
    static const uint8_t get_method[] = "GET";
    static const uint8_t m_path[] = "/m";
    static const uint8_t s_path[] = "/s";
    static const char one_shot[] = "GET /m HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    static const char stream_open[] = "GET /s HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n";
    vyl_managed_static_config config = {0};
    vyl_managed_static_server* server = NULL;
    vyl_managed_static_close_counts counts = {0};
    uint16_t port = 0;
    char response[4096] = {0};

    config.abi_version = VYL_MANAGED_STATIC_ABI_VERSION;
    config.struct_size = sizeof(config);
    config.request_head_timeout_ns = 5000000000u;
    config.body_progress_timeout_ns = 5000000000u;
    config.response_write_timeout_ns = 5000000000u;
    config.keep_alive_idle_timeout_ns = 30000000000u;
    config.shutdown_grace_ns = 5000000000u;
    config.drain_idle_linger_ns = 200000000u;
    config.max_accepts_per_turn = 8;
    config.max_events_per_turn = 64;
    config.byte_budget_per_turn = 65536;
    config.poll_wait_ms = 10;
    config.port = 0;

    assert(vyl_managed_static_new(&config, &server) == VYL_HTTP1_OK);
    assert(server != NULL);
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){.data = get_method, .length = sizeof(get_method) - 1},
            (vyl_http1_bytes){.data = m_path, .length = sizeof(m_path) - 1},
            (uint64_t)-1
        ) == VYL_HTTP1_OK
    );
    assert(
        vyl_managed_static_register_managed_route(
            server,
            (vyl_http1_bytes){.data = get_method, .length = sizeof(get_method) - 1},
            (vyl_http1_bytes){.data = s_path, .length = sizeof(s_path) - 1},
            (uint64_t)-1
        ) == VYL_HTTP1_OK
    );
    assert(vyl_managed_static_start(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_local_addr(server, &port) == VYL_HTTP1_OK);
    assert(port != 0);

    ft_queue incoming = {0};
    ft_queue streams = {0};
    ft_queue_init(&incoming);
    ft_queue_init(&streams);
    pthread_barrier_t start = {0};
    assert(pthread_barrier_init(&start, NULL, FT_POLLERS + 3) == 0);
    ft_poller_arg poller_arg = {.server = server, .start = &start, .incoming = &incoming};
    ft_worker_arg worker_arg =
        {.server = server, .start = &start, .incoming = &incoming, .streams = &streams};
    pthread_t pollers[FT_POLLERS] = {0};
    pthread_t responder = 0;
    pthread_t writer = 0;
    for (size_t i = 0; i < FT_POLLERS; i++) {
        assert(pthread_create(&pollers[i], NULL, ft_poller_main, &poller_arg) == 0);
    }
    assert(pthread_create(&responder, NULL, ft_responder_main, &worker_arg) == 0);
    assert(pthread_create(&writer, NULL, ft_writer_main, &worker_arg) == 0);
    ft_barrier_wait(&start);

    for (int i = 0; i < FT_ONE_SHOTS; i++) {
        int fd = connect_loopback(port);
        assert(send(fd, one_shot, sizeof(one_shot) - 1, 0) == (ssize_t)(sizeof(one_shot) - 1));
        read_to_end(fd, response, sizeof(response));
        assert(close(fd) == 0);
        assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
        assert(strstr(response, "managed-hi") != NULL);
    }
    {
        int fd = connect_loopback(port);
        assert(
            send(fd, stream_open, sizeof(stream_open) - 1, 0) == (ssize_t)(sizeof(stream_open) - 1)
        );
        read_to_end(fd, response, sizeof(response));
        assert(close(fd) == 0);
        assert(strstr(response, "HTTP/1.1 200 OK") != NULL);
        assert(strstr(response, "Transfer-Encoding: chunked") != NULL);
        assert(strstr(response, "2\r\nhi\r\n") != NULL);
        assert(strstr(response, "2\r\nyo\r\n") != NULL);
        assert(strstr(response, "0\r\n\r\n") != NULL);
    }

    ft_queue_finish(&incoming);
    ft_queue_finish(&streams);
    for (size_t i = 0; i < FT_POLLERS; i++) {
        ft_join_timed(pollers[i]);
    }
    ft_join_timed(responder);
    ft_join_timed(writer);

    assert(atomic_load(&g_observed) == FT_EXCHANGES);
    assert(atomic_load(&g_responded) == FT_ONE_SHOTS);
    assert(atomic_load(&g_begun) == FT_STREAMS);
    assert(atomic_load(&g_written) == FT_STREAMS);

    assert(vyl_managed_static_stop_accepting(server) == VYL_HTTP1_OK);
    assert(vyl_managed_static_join(server, &counts) == VYL_HTTP1_OK);
    assert(counts.completed == FT_EXCHANGES);
    assert(
        counts.completed + counts.peer_closed + counts.reset + counts.timeout +
            counts.protocol_error + counts.response_overflow + counts.transport_error +
            counts.shutdown ==
        FT_EXCHANGES
    );
    assert(vyl_managed_static_destroy(server) == VYL_HTTP1_OK);
    assert(pthread_barrier_destroy(&start) == 0);
    return 0;
}
