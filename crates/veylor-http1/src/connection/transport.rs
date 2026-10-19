#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoOperation {
    Read,
    Write,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IoResult {
    Progress { transferred_bytes: usize },
    WouldBlock,
    Interrupted,
    EndOfStream,
    Cancelled,
    Closed,
    Error { code: i32 },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IoCompletion {
    pub connection_id: u64,
    pub operation_id: u64,
    pub operation: IoOperation,
    pub result: IoResult,
}

impl IoResult {
    pub(crate) fn is_valid(self) -> bool {
        match self {
            Self::Progress { transferred_bytes } => transferred_bytes > 0,
            Self::Error { code } => code != 0,
            Self::WouldBlock
            | Self::Interrupted
            | Self::EndOfStream
            | Self::Cancelled
            | Self::Closed => true,
        }
    }
}

impl IoCompletion {
    pub(crate) fn valid_for(
        self,
        connection_id: u64,
        operation_id: Option<u64>,
        operation: IoOperation,
    ) -> bool {
        self.connection_id == connection_id
            && self.operation == operation
            && operation_id.is_none_or(|last| self.operation_id > last)
            && self.result.is_valid()
    }
}
