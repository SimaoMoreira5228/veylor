use std::io;

use event_listener::EventListener;

use crate::client::StreamingBody;
use crate::h1::H1Exchange;
use crate::h2::{HeaderList, UploadState};

const CHUNK: usize = 16 * 1024;

pub enum Next {
    More,
    Wait(EventListener),
    Tail(Option<HeaderList>),
}

pub struct Pull {
    pub written: usize,
    pub next: Next,
}

impl StreamingBody {
    pub fn pull(&self, exchange: &mut H1Exchange) -> io::Result<Pull> {
        let upload = self.upload();
        let listener = upload.listen_data();
        let mut written = 0usize;
        let mut failure = None;

        let state = upload.drain(|data, _| {
            if data.is_empty() {
                return 0;
            }

            let take = data.len().min(CHUNK);

            match exchange.encode_body(&data[..take]) {
                Ok(encoded) => {
                    written = encoded;

                    take
                }
                Err(error) => {
                    failure = Some(error);

                    0
                }
            }
        });

        if let Some(error) = failure {
            return Err(error);
        }

        let next = match state {
            UploadState::Aborted => {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    "streaming request body was abandoned",
                ));
            }
            UploadState::Open if written > 0 => Next::More,
            UploadState::Open => Next::Wait(listener),
            UploadState::Finished => Next::Tail(None),
            UploadState::Trailers(trailers) => Next::Tail(Some(trailers)),
        };

        Ok(Pull { written, next })
    }
}
