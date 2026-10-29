#include "veylor_http1.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>


/* NOTE: cross-boundary layout pins (all declared targets are 64-bit, so
   size_t is 8 bytes): the C declarations must stay byte-identical to the Rust
   #[repr(C)] originals. Any field addition, removal, or reorder breaks these. */
_Static_assert(
    sizeof(vyl_h2_field_to_encode) == 48,
    "H2 field layout must match Rust HpackFieldToEncode"
);
_Static_assert(
    sizeof(vyl_h2_field) == 32,
    "H2 decoded field layout must match Rust HpackHeader"
);
_Static_assert(
    sizeof(vyl_h2_action) == 56,
    "H2 action layout must match Rust H2FrameAction"
);
_Static_assert(
    sizeof(vyl_h2_response_class) == 24,
    "H2 response class layout must match Rust ResponseClass"
);
 
#define WIRE_CAP 4096u
#define POLL_ACTIONS 16u
#define POLL_FIELDS 64u
#define POLL_DATA 4096u

#define CHECK(step, cond)                                                                          \
    do {                                                                                           \
        if (!(cond)) {                                                                             \
            fprintf(stderr, "h2_exchange: %s failed at line %d\n", step, __LINE__);                 \
            return 1;                                                                              \
        }                                                                                          \
    } while (0)

static vyl_h2_field_to_encode encode_field(const char* name, const char* value) {
    vyl_h2_field_to_encode field = {0};
    field.name.data = (const uint8_t*)name;
    field.name.length = strlen(name);
    field.value.data = (const uint8_t*)value;
    field.value.length = strlen(value);
    field.representation = VYL_H2_WITHOUT_INDEXING;
    return field;
}

static int ack_client(vyl_h2_client* client, const char* step, size_t transferred) {
    size_t pending = SIZE_MAX;
    CHECK(step, vyl_h2_client_acknowledge(client, transferred, &pending) == VYL_HTTP1_OK);
    CHECK(step, pending == 0);
    return 0;
}

static int ack_server(vyl_h2_server* server, const char* step, size_t transferred) {
    size_t pending = SIZE_MAX;
    CHECK(step, vyl_h2_server_acknowledge(server, transferred, &pending) == VYL_HTTP1_OK);
    CHECK(step, pending == 0);
    return 0;
}

typedef struct {
    size_t consumed;
    size_t actions;
    size_t fields;
} poll_counts;

static int feed_client(
    vyl_h2_client* client,
    const char* step,
    const uint8_t* input,
    size_t input_length,
    size_t action_cap,
    size_t field_cap,
    size_t data_cap,
    vyl_h2_action* actions,
    vyl_h2_field* fields,
    uint8_t* data,
    poll_counts* counts,
    size_t* actions_required,
    size_t* fields_required,
    size_t* data_required,
    uint32_t* connection_error,
    int expected
) {
    size_t out_actions = SIZE_MAX;
    size_t out_fields = SIZE_MAX;
    int rc = vyl_h2_client_poll_feed(
        client,
        input,
        input_length,
        &counts->consumed,
        actions,
        action_cap,
        fields,
        field_cap,
        data,
        data_cap,
        &out_actions,
        &out_fields,
        actions_required,
        fields_required,
        data_required,
        connection_error
    );
    CHECK(step, rc == expected);
    counts->actions = out_actions;
    counts->fields = out_fields;
    return 0;
}

static int feed_server(
    vyl_h2_server* server,
    const char* step,
    const uint8_t* input,
    size_t input_length,
    size_t action_cap,
    size_t field_cap,
    size_t data_cap,
    vyl_h2_action* actions,
    vyl_h2_field* fields,
    uint8_t* data,
    poll_counts* counts,
    size_t* actions_required,
    size_t* fields_required,
    size_t* data_required,
    uint32_t* connection_error,
    int expected
) {
    size_t out_actions = SIZE_MAX;
    size_t out_fields = SIZE_MAX;
    int rc = vyl_h2_server_poll_feed(
        server,
        input,
        input_length,
        &counts->consumed,
        actions,
        action_cap,
        fields,
        field_cap,
        data,
        data_cap,
        &out_actions,
        &out_fields,
        actions_required,
        fields_required,
        data_required,
        connection_error
    );
    CHECK(step, rc == expected);
    counts->actions = out_actions;
    counts->fields = out_fields;
    return 0;
}

static int field_equals(
    const uint8_t* data,
    vyl_h2_field field,
    const char* name,
    const char* value
) {
    return field.name_length == strlen(name) && field.value_length == strlen(value) &&
           memcmp(data + field.name_offset, name, field.name_length) == 0 &&
           memcmp(data + field.value_offset, value, field.value_length) == 0;
}

static int test_h2_exchange(void) {
    vyl_h2_client* client = NULL;
    vyl_h2_server* server = NULL;
    static uint8_t wire[WIRE_CAP];
    static vyl_h2_action actions[POLL_ACTIONS];
    static vyl_h2_field fields[POLL_FIELDS];
    static uint8_t poll_data[POLL_DATA];
    static uint8_t body[64];
    static uint8_t server_ack[32];
    static uint8_t client_ack[32];
    poll_counts counts = {0};
    size_t actions_required = 0;
    size_t fields_required = 0;
    size_t data_required = 0;
    uint32_t connection_error = 0;
    size_t written = SIZE_MAX;
    size_t required = SIZE_MAX;
    size_t consumed = 0;
    size_t server_ack_length = 0;
    size_t client_ack_length = 0;
    uint32_t stream_id = 0;
    size_t body_length = 0;

    CHECK("create client", vyl_h2_client_new(&client) == VYL_HTTP1_OK);
    CHECK("create client", client != NULL);
    CHECK("create server", vyl_h2_server_new(&server) == VYL_HTTP1_OK);
    CHECK("create server", server != NULL);

    CHECK(
        "client preface",
        vyl_h2_client_connection_preface(wire, sizeof(wire), &written, &required) == VYL_HTTP1_OK
    );
    CHECK("client preface", written == 24);
    CHECK(
        "server preface feed",
        feed_server(
            server,
            "server preface feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_NEED_MORE
        ) == 0
    );
    CHECK("server preface feed", counts.consumed == 24);

    CHECK(
        "client settings",
        vyl_h2_client_send_settings(client, NULL, 0, wire, sizeof(wire), &written, &required) ==
            VYL_HTTP1_OK
    );
    CHECK("client settings ack", ack_client(client, "client settings ack", written) == 0);
    CHECK(
        "server settings feed",
        feed_server(
            server,
            "server settings feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_OK
        ) == 0
    );
    CHECK("server settings feed", counts.actions == 1);
    CHECK("server settings feed", actions[0].kind == VYL_H2_ACTION_SETTINGS_ACK_REQUIRED);
    CHECK("server settings feed", counts.consumed == written);
    CHECK(
        "server settings ack",
        vyl_h2_server_send_settings_ack(
            server, server_ack, sizeof(server_ack), &server_ack_length, &required
        ) == VYL_HTTP1_OK
    );
    CHECK(
        "server settings ack",
        ack_server(server, "server settings ack", server_ack_length) == 0
    );

    CHECK(
        "server settings",
        vyl_h2_server_send_settings(server, NULL, 0, wire, sizeof(wire), &written, &required) ==
            VYL_HTTP1_OK
    );
    CHECK("server settings ack", ack_server(server, "server settings ack", written) == 0);
    CHECK(
        "client settings feed",
        feed_client(
            client,
            "client settings feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_OK
        ) == 0
    );
    CHECK("client settings feed", counts.actions == 1);
    CHECK("client settings feed", actions[0].kind == VYL_H2_ACTION_SETTINGS_ACK_REQUIRED);
    CHECK("client settings feed", counts.consumed == written);
    CHECK(
        "client settings ack",
        vyl_h2_client_send_settings_ack(
            client, client_ack, sizeof(client_ack), &client_ack_length, &required
        ) == VYL_HTTP1_OK
    );
    CHECK("client settings ack", ack_client(client, "client settings ack", client_ack_length) == 0);

    CHECK(
        "client settings ack feed",
        feed_client(
            client,
            "client settings ack feed",
            server_ack,
            server_ack_length,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_NEED_MORE
        ) == 0
    );
    CHECK("client settings ack feed", counts.consumed == server_ack_length);
    CHECK(
        "server settings ack feed",
        feed_server(
            server,
            "server settings ack feed",
            client_ack,
            client_ack_length,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_NEED_MORE
        ) == 0
    );
    CHECK("server settings ack feed", counts.consumed == client_ack_length);

    {
        vyl_h2_field_to_encode request_fields[] = {
            encode_field(":method", "GET"),
            encode_field(":scheme", "https"),
            encode_field(":path", "/hello"),
            encode_field(":authority", "example.com"),
        };
        CHECK(
            "client start request",
            vyl_h2_client_start_request(
                client,
                request_fields,
                4,
                VYL_H2_END_STREAM,
                wire,
                sizeof(wire),
                &written,
                &stream_id,
                &required
            ) == VYL_HTTP1_OK
        );
        CHECK("client start request", stream_id == 1);
    }
    CHECK("client request ack", ack_client(client, "client request ack", written) == 0);
    CHECK(
        "server request feed",
        feed_server(
            server,
            "server request feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_OK
        ) == 0
    );
    CHECK("server request feed", counts.actions == 1);
    CHECK("server request feed", actions[0].kind == VYL_H2_ACTION_HEADERS);
    CHECK("server request feed", actions[0].stream_id == 1);
    CHECK("server request feed", actions[0].flags == VYL_H2_END_STREAM);
    CHECK("server request feed", actions[0].field_count == 4);
    CHECK("server request feed", counts.consumed == written);

    {
        vyl_h2_validated_request validated = {0};
        CHECK(
            "server accept request",
            vyl_h2_server_accept_request(
                server, 1, actions[0].field_start, actions[0].field_count, &validated
            ) == VYL_HTTP1_OK
        );
        CHECK(
            "server accept request",
            field_equals(poll_data, fields[validated.method_field], ":method", "GET")
        );
        CHECK(
            "server accept request",
            field_equals(poll_data, fields[validated.path_field], ":path", "/hello")
        );
    }

    {
        vyl_h2_field_to_encode response_fields[] = {
            encode_field(":status", "200"),
            encode_field("content-length", "11"),
        };
        CHECK(
            "server send response",
            vyl_h2_server_send_response(
                server, 1, response_fields, 2, 0, wire, sizeof(wire), &written, &required
            ) == VYL_HTTP1_OK
        );
    }
    CHECK("server response ack", ack_server(server, "server response ack", written) == 0);

    CHECK(
        "client small poll retry",
        feed_client(
            client,
            "client small poll retry",
            wire,
            written,
            0,
            0,
            0,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_BUFFER_TOO_SMALL
        ) == 0
    );
    CHECK("client small poll retry", counts.consumed == written);
    CHECK("client small poll retry", actions_required >= 1);
    CHECK(
        "client response feed",
        feed_client(
            client,
            "client response feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_OK
        ) == 0
    );
    CHECK("client response feed", counts.consumed == 0);
    CHECK("client response feed", counts.actions == 1);
    CHECK("client response feed", actions[0].kind == VYL_H2_ACTION_HEADERS);
    CHECK("client response feed", actions[0].stream_id == 1);

    {
        vyl_h2_response_class class = {0};
        CHECK(
            "client classify headers",
            vyl_h2_client_classify_headers(
                client, 1, actions[0].field_start, actions[0].field_count, actions[0].flags, &class
            ) == VYL_HTTP1_OK
        );
        CHECK("client classify headers", class.kind == VYL_H2_RESPONSE_FINAL);
        CHECK("client classify headers", class.status == 200);
        CHECK("client classify headers", class.phase == VYL_H2_PHASE_CONTENT);
        CHECK("client classify headers", class.content_length == 11);
        CHECK("client classify headers", class.has_content_length == 1);
        CHECK("client classify headers", class.end_stream == 0);
    }

    {
        static const uint8_t first[] = "hello ";
        static const uint8_t second[] = "world";
        uint32_t end_applied = 0;
        uint32_t phase = 0;
        CHECK(
            "server send data head",
            vyl_h2_server_send_data(
                server,
                1,
                (vyl_http1_bytes){.data = first, .length = sizeof(first) - 1},
                0,
                wire,
                sizeof(wire),
                &consumed,
                &written,
                &end_applied,
                &required
            ) == VYL_HTTP1_OK
        );
        CHECK("server send data head", consumed == sizeof(first) - 1);
        CHECK("server send data head", end_applied == 0);
        CHECK("server data head ack", ack_server(server, "server data head ack", written) == 0);
        CHECK(
            "client data head feed",
            feed_client(
                client,
                "client data head feed",
                wire,
                written,
                POLL_ACTIONS,
                POLL_FIELDS,
                POLL_DATA,
                actions,
                fields,
                poll_data,
                &counts,
                &actions_required,
                &fields_required,
                &data_required,
                &connection_error,
                VYL_HTTP1_OK
            ) == 0
        );
        CHECK("client data head feed", counts.actions == 1);
        CHECK("client data head feed", actions[0].kind == VYL_H2_ACTION_DATA);
        CHECK("client data head feed", actions[0].stream_id == 1);
        CHECK("client data head feed", actions[0].data_length == sizeof(first) - 1);
        assert(actions[0].data_offset + actions[0].data_length <= sizeof(poll_data));
        memcpy(body, poll_data + actions[0].data_offset, actions[0].data_length);
        body_length = actions[0].data_length;
        CHECK(
            "client account data head",
            vyl_h2_client_account_data(
                client, 1, actions[0].data_length, actions[0].flags, &phase
            ) == VYL_HTTP1_OK
        );
        CHECK("client account data head", phase == VYL_H2_PHASE_CONTENT);

        CHECK(
            "server send data tail",
            vyl_h2_server_send_data(
                server,
                1,
                (vyl_http1_bytes){.data = second, .length = sizeof(second) - 1},
                VYL_H2_END_STREAM,
                wire,
                sizeof(wire),
                &consumed,
                &written,
                &end_applied,
                &required
            ) == VYL_HTTP1_OK
        );
        CHECK("server send data tail", consumed == sizeof(second) - 1);
        CHECK("server send data tail", end_applied == VYL_H2_END_STREAM);
        CHECK("server data tail ack", ack_server(server, "server data tail ack", written) == 0);
        CHECK(
            "client data tail feed",
            feed_client(
                client,
                "client data tail feed",
                wire,
                written,
                POLL_ACTIONS,
                POLL_FIELDS,
                POLL_DATA,
                actions,
                fields,
                poll_data,
                &counts,
                &actions_required,
                &fields_required,
                &data_required,
                &connection_error,
                VYL_HTTP1_OK
            ) == 0
        );
        CHECK("client data tail feed", counts.actions == 1);
        CHECK("client data tail feed", actions[0].kind == VYL_H2_ACTION_DATA);
        CHECK("client data tail feed", actions[0].stream_id == 1);
        CHECK("client data tail feed", actions[0].flags == VYL_H2_END_STREAM);
        assert(actions[0].data_offset + actions[0].data_length <= sizeof(poll_data));
        assert(body_length + actions[0].data_length <= sizeof(body));
        memcpy(body + body_length, poll_data + actions[0].data_offset, actions[0].data_length);
        body_length += actions[0].data_length;
        CHECK(
            "client account data tail",
            vyl_h2_client_account_data(
                client, 1, actions[0].data_length, actions[0].flags, &phase
            ) == VYL_HTTP1_OK
        );
        CHECK("client account data tail", phase == VYL_H2_PHASE_COMPLETE);
        CHECK("client data bytes", body_length == sizeof("hello world") - 1);
        CHECK("client data bytes", memcmp(body, "hello world", body_length) == 0);
    }

    {
        vyl_h2_field_to_encode second_fields[] = {
            encode_field(":method", "GET"),
            encode_field(":scheme", "https"),
            encode_field(":path", "/second"),
            encode_field(":authority", "example.com"),
        };
        CHECK(
            "client start second request",
            vyl_h2_client_start_request(
                client, second_fields, 4, 0, wire, sizeof(wire), &written, &stream_id, &required
            ) == VYL_HTTP1_OK
        );
        CHECK("client start second request", stream_id == 3);
    }
    CHECK(
        "client second request ack",
        ack_client(client, "client second request ack", written) == 0
    );
    CHECK(
        "server second request feed",
        feed_server(
            server,
            "server second request feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_OK
        ) == 0
    );
    CHECK("server second request feed", counts.actions == 1);
    CHECK("server second request feed", actions[0].kind == VYL_H2_ACTION_HEADERS);
    CHECK("server second request feed", actions[0].stream_id == 3);
    CHECK("server second request feed", actions[0].flags == 0);
    CHECK(
        "client cancel stream",
        vyl_h2_client_cancel_stream(client, 3, wire, sizeof(wire), &written, &required) ==
            VYL_HTTP1_OK
    );
    CHECK("client cancel ack", ack_client(client, "client cancel ack", written) == 0);
    CHECK(
        "server reset feed",
        feed_server(
            server,
            "server reset feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_OK
        ) == 0
    );
    CHECK("server reset feed", counts.actions == 1);
    CHECK("server reset feed", actions[0].kind == VYL_H2_ACTION_STREAM_RESET);
    CHECK("server reset feed", actions[0].stream_id == 3);
    CHECK("server reset feed", actions[0].error_code == 8);

    CHECK(
        "server goaway drain",
        vyl_h2_server_goaway_drain(server, 3, wire, sizeof(wire), &written, &required) ==
            VYL_HTTP1_OK
    );
    CHECK("server goaway ack", ack_server(server, "server goaway ack", written) == 0);
    CHECK(
        "client goaway feed",
        feed_client(
            client,
            "client goaway feed",
            wire,
            written,
            POLL_ACTIONS,
            POLL_FIELDS,
            POLL_DATA,
            actions,
            fields,
            poll_data,
            &counts,
            &actions_required,
            &fields_required,
            &data_required,
            &connection_error,
            VYL_HTTP1_OK
        ) == 0
    );
    CHECK("client goaway feed", counts.actions == 1);
    CHECK("client goaway feed", actions[0].kind == VYL_H2_ACTION_GOAWAY);
    CHECK("client goaway feed", actions[0].stream_id == 3);
    CHECK("client goaway feed", actions[0].error_code == 0);

    {
        vyl_h2_field_to_encode late_fields[] = {
            encode_field(":method", "GET"),
            encode_field(":scheme", "https"),
            encode_field(":path", "/late"),
            encode_field(":authority", "example.com"),
        };
        CHECK(
            "client post-goaway request",
            vyl_h2_client_start_request(
                client, late_fields, 4, VYL_H2_END_STREAM, wire, sizeof(wire), &written, &stream_id,
                &required
            ) == VYL_HTTP1_BAD_STATE
        );
    }

    CHECK("destroy client", vyl_h2_client_destroy(client) == VYL_HTTP1_OK);
    CHECK("destroy server", vyl_h2_server_destroy(server) == VYL_HTTP1_OK);
    return 0;
}

int main(void) {
    if (test_h2_exchange() != 0) {
        return 1;
    }
    return 0;
}
