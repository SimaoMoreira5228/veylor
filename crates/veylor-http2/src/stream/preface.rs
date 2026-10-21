use super::{H2Role, StreamTable};

pub const CLIENT_CONNECTION_PREFACE: &[u8; 24] = b"PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClientPrefaceError {
    WrongRole,
    InvalidByte { offset: usize },
    AlreadyComplete,
}

impl<'a> StreamTable<'a> {
    pub fn receive_client_preface_fragment(
        &mut self,
        input: &[u8],
    ) -> Result<usize, ClientPrefaceError> {
        if self.role != H2Role::Server {
            return Err(ClientPrefaceError::WrongRole);
        }
        if self.client_preface_failed {
            return Err(ClientPrefaceError::AlreadyComplete);
        }
        if self.client_preface_bytes == CLIENT_CONNECTION_PREFACE.len() {
            return Err(ClientPrefaceError::AlreadyComplete);
        }

        let remaining = CLIENT_CONNECTION_PREFACE.len() - self.client_preface_bytes;
        let consumed = input.len().min(remaining);
        for (offset, byte) in input[..consumed].iter().enumerate() {
            let preface_offset = self.client_preface_bytes + offset;
            if *byte != CLIENT_CONNECTION_PREFACE[preface_offset] {
                self.client_preface_failed = true;
                return Err(ClientPrefaceError::InvalidByte {
                    offset: preface_offset,
                });
            }
        }
        self.client_preface_bytes += consumed;
        Ok(consumed)
    }

    pub const fn client_preface_complete(&self) -> bool {
        self.client_preface_bytes == CLIENT_CONNECTION_PREFACE.len() && !self.client_preface_failed
    }

    pub const fn client_preface_progress(&self) -> usize {
        self.client_preface_bytes
    }
}
