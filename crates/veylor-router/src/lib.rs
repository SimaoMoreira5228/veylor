mod path;
mod table;

pub use path::RouteCapture;
pub use table::{RouteBodyPolicy, RouteConfig, RouteKind, RouteMatch, RouteSlot, RouteTable};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteError {
    Invalid,
    InvalidRequest,
    Full,
    Frozen,
    NotFrozen,
    Duplicate,
    NotFound,
    MethodNotAllowed,
    CaptureCapacity,
}
