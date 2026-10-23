#[allow(unsafe_code)]
mod api;
#[allow(unsafe_code)]
mod dispatch;

#[cfg(test)]
#[allow(unsafe_code)]
mod tests;

pub use api::{
    vyl_managed_static_destroy, vyl_managed_static_join, vyl_managed_static_local_addr,
    vyl_managed_static_new, vyl_managed_static_register_managed_route,
    vyl_managed_static_register_managed_route_with_trailers,
    vyl_managed_static_register_static_route,
    vyl_managed_static_register_static_route_with_body_cap, vyl_managed_static_start,
    vyl_managed_static_stop_accepting, vyl_managed_static_stop_force,
};
pub use dispatch::{
    vyl_managed_static_host_cancel, vyl_managed_static_host_pull_body,
    vyl_managed_static_host_pull_trailers, vyl_managed_static_host_respond,
    vyl_managed_static_poll_events,
};
use veylor_server::{CloseCounts, RunningServer, Server, ServerConfig};

use crate::abi::VylResult;

pub const VYL_MANAGED_STATIC_ABI_VERSION: u32 = 1;
pub const VYL_MANAGED_STATIC_EVENT_REQUEST: u32 = 0;
pub const VYL_MANAGED_STATIC_EVENT_SHUTDOWN: u32 = 1;

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
    Ready(Server),
    Running(RunningServer),
    Joined,
}

impl VylManagedStaticConfig {
    pub(super) fn to_server_config(self) -> Result<(ServerConfig, u16), VylResult> {
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
        Ok((config, self.port))
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
