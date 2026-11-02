mod path;
mod table;
#[cfg(test)]
mod tests;

pub use path::{
    MAX_ROUTE_CAPTURES, RouteCapture, capture_route, compare_route_patterns, match_route,
    matches_route, valid_route_pattern,
};
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
