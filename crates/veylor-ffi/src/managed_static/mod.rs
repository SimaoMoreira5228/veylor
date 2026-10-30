#[allow(unsafe_code)]
mod api;
#[allow(unsafe_code)]
mod dispatch;
#[allow(unsafe_code)]
mod native;
#[allow(unsafe_code)]
mod register;
#[allow(unsafe_code)]
mod stream;
#[allow(unsafe_code)]
mod tunnel;

#[cfg(test)]
#[allow(unsafe_code)]
mod tests;

pub use api::{
    vyl_managed_static_destroy, vyl_managed_static_enable_h2c, vyl_managed_static_enable_tls,
    vyl_managed_static_h2c_addr, vyl_managed_static_join, vyl_managed_static_local_addr,
    vyl_managed_static_new, vyl_managed_static_rotate_tls, vyl_managed_static_set_worker_count,
    vyl_managed_static_start, vyl_managed_static_stop_accepting, vyl_managed_static_stop_force,
};
pub use dispatch::{
    vyl_managed_static_host_cancel, vyl_managed_static_host_pull_body,
    vyl_managed_static_host_pull_trailers, vyl_managed_static_host_respond,
    vyl_managed_static_host_respond_with_headers, vyl_managed_static_poll_events,
};
pub use native::{
    VylManagedStaticNativeHandler, VylManagedStaticNativeHandlerFn, VylManagedStaticNativeRequest,
    VylManagedStaticNativeResponse, vyl_managed_static_register_native_route,
    vyl_managed_static_register_native_route_with_gzip,
};
pub use register::{
    vyl_managed_static_register_file_mount, vyl_managed_static_register_managed_route,
    vyl_managed_static_register_managed_route_with_trailers,
    vyl_managed_static_register_static_route,
    vyl_managed_static_register_static_route_with_body_cap,
    vyl_managed_static_register_static_route_with_gzip,
};
use std::net::{IpAddr, SocketAddr};
use std::slice;
pub use stream::{
    vyl_managed_static_host_begin_stream, vyl_managed_static_host_begin_stream_with_headers,
    vyl_managed_static_host_end_stream, vyl_managed_static_host_end_stream_with_trailers,
    vyl_managed_static_host_is_writable, vyl_managed_static_host_write_stream,
};
pub use tunnel::{
    vyl_managed_static_host_tunnel_close, vyl_managed_static_host_tunnel_read,
    vyl_managed_static_host_tunnel_write, vyl_managed_static_host_upgrade_websocket,
    vyl_managed_static_host_upgrade_websocket_with_protocol,
};

use veylor_server::{
    CloseCounts, MAX_CONNECTIONS, MAX_WORKERS, NativeHandler, RunningServer, Server, ServerConfig,
};

use crate::abi::VylResult;

pub const VYL_MANAGED_STATIC_ABI_VERSION: u32 = 2;
pub const VYL_MANAGED_STATIC_EVENT_REQUEST: u32 = 0;
pub const VYL_MANAGED_STATIC_EVENT_SHUTDOWN: u32 = 1;
// NOTE: V1 structs stay size-pinned: both reuse VylManagedStaticEvent fields (see the
// struct docs), never grow it.
pub const VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE: u32 = 2;
pub const VYL_MANAGED_STATIC_EVENT_WRITABLE: u32 = 3;
pub const VYL_MANAGED_STATIC_EVENT_TUNNEL_DATA: u32 = 4;
pub const VYL_MANAGED_STATIC_EVENT_TUNNEL_CLOSED: u32 = 5;

#[repr(C)]
#[derive(Clone, Copy, Debug)]
pub struct VylManagedStaticConfig {
    pub abi_version: u32,
    pub struct_size: usize,
    pub request_head_timeout_ns: u64,
    pub body_progress_timeout_ns: u64,
    pub response_write_timeout_ns: u64,
    pub keep_alive_idle_timeout_ns: u64,
    pub shutdown_grace_ns: u64,
    pub drain_idle_linger_ns: u64,
    pub max_accepts_per_turn: u64,
    pub max_events_per_turn: u64,
    pub byte_budget_per_turn: u64,
    pub poll_wait_ms: i32,
    pub port: u16,
    pub reserved: u32,
    pub address: crate::abi::VylBytes,
    pub max_connections: u64,
    pub worker_count: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylManagedStaticCloseCounts {
    pub completed: u64,
    pub peer_closed: u64,
    pub reset: u64,
    pub timeout: u64,
    pub protocol_error: u64,
    pub response_overflow: u64,
    pub transport_error: u64,
    pub shutdown: u64,
}

impl From<CloseCounts> for VylManagedStaticCloseCounts {
    fn from(counts: CloseCounts) -> Self {
        Self {
            completed: counts.completed,
            peer_closed: counts.peer_closed,
            reset: counts.reset,
            timeout: counts.timeout,
            protocol_error: counts.protocol_error,
            response_overflow: counts.response_overflow,
            transport_error: counts.transport_error,
            shutdown: counts.shutdown,
        }
    }
}

pub struct VylManagedStaticServer {
    pub(super) state: ManagedStaticState,
}

pub(super) enum ManagedStaticState {
    Ready(Box<ReadyServer>),
    Running(RunningServer),
    Joined,
}

// NOTE: pre-start server. The bound handle serves eager registration validation and the
// pre-start local-addr query; `config`/`addr` plus the staged `h2c_port`/`worker_count`
// and the recorded `ops` replay a fresh bind at start when non-default settings apply.
// The default path starts the bound handle untouched, so stock behavior never rebinds.
pub(super) struct ReadyServer {
    pub(super) server: Server,
    pub(super) config: ServerConfig,
    pub(super) addr: std::net::SocketAddr,
    pub(super) h2c_port: Option<u16>,
    pub(super) worker_count: usize,
    pub(super) tls: Option<(Vec<u8>, Vec<u8>, usize)>,
    pub(super) ops: Vec<ReadyRouteOp>,
}

// NOTE: validated registration replay for the start-time rebind. Every entry already
// passed eager validation, so replay with identical bytes must succeed; failures still
// map fail-closed. `u64::MAX` stays the uncapped body sentinel on static and managed
// entries alike: no new variant, the standing cap-less answer.
pub(super) enum ReadyRouteOp {
    Static {
        method: Vec<u8>,
        path: Vec<u8>,
        status: u16,
        reason: Vec<u8>,
        body: Vec<u8>,
        max_body_bytes: u64,
    },
    Gzip {
        method: Vec<u8>,
        path: Vec<u8>,
        status: u16,
        reason: Vec<u8>,
        body: Vec<u8>,
        max_body_bytes: u64,
    },
    Managed {
        method: Vec<u8>,
        path: Vec<u8>,
        max_body_bytes: u64,
    },
    ManagedWithTrailers {
        method: Vec<u8>,
        path: Vec<u8>,
        max_body_bytes: u64,
        trailers: Vec<Vec<u8>>,
    },
    FileMount {
        prefix: Vec<u8>,
        root: std::path::PathBuf,
    },
    Native {
        method: Vec<u8>,
        path: Vec<u8>,
        handler: NativeHandler,
        user_data: usize,
        gzip: bool,
    },
}

impl ReadyRouteOp {
    pub(super) fn apply(&self, server: &mut Server) -> Result<(), VylResult> {
        let result = match self {
            Self::Static {
                method,
                path,
                status,
                reason,
                body,
                max_body_bytes,
            } => server.register_route(method, path, *status, reason, body, *max_body_bytes),
            Self::Gzip {
                method,
                path,
                status,
                reason,
                body,
                max_body_bytes,
            } => server.register_gzip_route(method, path, *status, reason, body, *max_body_bytes),
            Self::Managed {
                method,
                path,
                max_body_bytes,
            } => server.register_managed(method, path, *max_body_bytes),
            Self::ManagedWithTrailers {
                method,
                path,
                max_body_bytes,
                trailers,
            } => {
                let names: Vec<&[u8]> = trailers.iter().map(Vec::as_slice).collect();
                server.register_managed_with_trailers(method, path, *max_body_bytes, &names)
            }
            Self::FileMount { prefix, root } => server.register_file_mount(prefix, root),
            Self::Native {
                method,
                path,
                handler,
                user_data,
                gzip,
            } => {
                if *gzip {
                    server.register_native_gzip(method, path, Some(*handler), *user_data)
                } else {
                    server.register_native(method, path, Some(*handler), *user_data)
                }
            }
        };
        result.map_err(server_error_to_result)
    }
}

#[allow(unsafe_code)]
impl VylManagedStaticConfig {
    pub(super) fn to_server_config(self) -> Result<(ServerConfig, SocketAddr), VylResult> {
        if self.abi_version != VYL_MANAGED_STATIC_ABI_VERSION {
            return Err(VylResult::InvalidArgument);
        }
        if self.struct_size != std::mem::size_of::<Self>() {
            return Err(VylResult::InvalidArgument);
        }
        if self.request_head_timeout_ns == 0 {
            return Err(VylResult::InvalidArgument);
        }
        if self.body_progress_timeout_ns == 0 {
            return Err(VylResult::InvalidArgument);
        }
        if self.response_write_timeout_ns == 0 {
            return Err(VylResult::InvalidArgument);
        }
        if self.keep_alive_idle_timeout_ns == 0 {
            return Err(VylResult::InvalidArgument);
        }
        if self.max_accepts_per_turn == 0 {
            return Err(VylResult::InvalidArgument);
        }
        if self.max_events_per_turn == 0 {
            return Err(VylResult::InvalidArgument);
        }
        if self.byte_budget_per_turn == 0 {
            return Err(VylResult::InvalidArgument);
        }
        if self.poll_wait_ms <= 0 {
            return Err(VylResult::InvalidArgument);
        }
        let mut config = ServerConfig::default();
        config.timeouts.request_head_ns = self.request_head_timeout_ns;
        config.timeouts.body_progress_ns = self.body_progress_timeout_ns;
        config.timeouts.response_write_ns = self.response_write_timeout_ns;
        config.timeouts.keep_alive_idle_ns = self.keep_alive_idle_timeout_ns;
        config.shutdown_grace_ns = self.shutdown_grace_ns;
        config.drain_idle_linger_ns = self.drain_idle_linger_ns;
        config.turn.max_accepts_per_turn =
            usize::try_from(self.max_accepts_per_turn).map_err(|_| VylResult::InvalidArgument)?;
        config.turn.max_events_per_turn =
            usize::try_from(self.max_events_per_turn).map_err(|_| VylResult::InvalidArgument)?;
        config.turn.byte_budget_per_turn = self.byte_budget_per_turn;
        config.poll_wait_ms = self.poll_wait_ms;

        let address = if self.address.length == 0 {
            IpAddr::from(std::net::Ipv4Addr::LOCALHOST)
        } else {
            if self.address.data.is_null() || self.address.length > 64 {
                return Err(VylResult::InvalidArgument);
            }
            let bytes = unsafe { slice::from_raw_parts(self.address.data, self.address.length) };
            std::str::from_utf8(bytes)
                .ok()
                .and_then(|text| text.parse::<IpAddr>().ok())
                .ok_or(VylResult::InvalidArgument)?
        };

        if self.max_connections != 0 {
            let limit = usize::try_from(self.max_connections)
                .ok()
                .filter(|value| (1..=MAX_CONNECTIONS).contains(value))
                .ok_or(VylResult::InvalidArgument)?;
            config.max_connections = limit;
        }

        if self.worker_count != 0 {
            let count = usize::try_from(self.worker_count)
                .ok()
                .filter(|value| (1..=MAX_WORKERS).contains(value))
                .ok_or(VylResult::InvalidArgument)?;
            config.worker_count = count;
        }

        Ok((config, SocketAddr::new(address, self.port)))
    }
}

pub(super) fn server_error_to_result(error: veylor_server::ServerError) -> VylResult {
    match error {
        veylor_server::ServerError::InvalidConfig => VylResult::InvalidArgument,
        veylor_server::ServerError::InvalidRoute => VylResult::InvalidArgument,
        veylor_server::ServerError::DuplicateRoute => VylResult::InvalidArgument,
        veylor_server::ServerError::RouteCapacity => VylResult::Capacity,
        veylor_server::ServerError::ResponseTooLarge => VylResult::BodyTooLong,
        veylor_server::ServerError::Bind(_) => VylResult::InternalError,
        veylor_server::ServerError::Poll(_) => VylResult::InternalError,
        veylor_server::ServerError::Thread(_) => VylResult::InternalError,
        veylor_server::ServerError::Join(_) => VylResult::InternalError,
    }
}

const _: () = assert!(std::mem::size_of::<VylManagedStaticConfig>() > 0);
const _: () = assert!(VYL_MANAGED_STATIC_EVENT_BODY_AVAILABLE == 2);
const _: () = assert!(VYL_MANAGED_STATIC_EVENT_WRITABLE == 3);
const _: () = assert!(std::mem::size_of::<VylManagedStaticCloseCounts>() == 8 * 8);
const _: () = assert!(std::mem::size_of::<VylManagedStaticToken>() == 4 * 4 + 8 * 2);
const _: () = assert!(std::mem::size_of::<VylManagedStaticPollReport>() == 8 * 4);
const _: () = assert!(std::mem::size_of::<VylManagedStaticTrailerReport>() == 8 * 5);

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylManagedStaticToken {
    pub worker_id: u32,
    pub slot: u32,
    pub request_seq: u32,
    pub stream_id: u32,
    pub worker_generation: u64,
    pub generation: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylManagedStaticEvent {
    pub kind: u32,
    pub token: VylManagedStaticToken,
    pub route_id: u64,
    pub method_len: usize,
    pub path_len: usize,
    pub method_offset: usize,
    pub path_offset: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylManagedStaticPollReport {
    pub copied: usize,
    pub events_required: usize,
    pub method_required: usize,
    pub path_required: usize,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct VylManagedStaticTrailerReport {
    pub fields: usize,
    pub names_copied: usize,
    pub values_copied: usize,
    pub names_required: usize,
    pub values_required: usize,
}
