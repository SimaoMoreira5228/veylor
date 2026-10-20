mod lifecycle;

pub use lifecycle::{
    ShutdownPhase, ShutdownResult, ShutdownState, close_connection, destroy, force_shutdown,
    process_deadline, register_connection, register_connections, request_shutdown,
};

#[cfg(test)]
mod tests;
