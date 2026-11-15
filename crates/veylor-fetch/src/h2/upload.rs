use std::io::{self, Read};
use std::sync::Arc;

use super::shared::{StreamShared, Ticket, UPLOAD_CHUNK};
use crate::budget::Budget;

pub(super) fn pump(
    shared: &Arc<StreamShared>,
    reader: &mut dyn Read,
    declared: Option<u64>,
    budget: &Budget,
) -> io::Result<()> {
    let mut remaining = declared;

    loop {
        let limit = match remaining {
            Some(0) => break,
            Some(left) => left.min(UPLOAD_CHUNK as u64) as usize,
            None => UPLOAD_CHUNK,
        };

        let mut chunk = match shared.take_chunk(budget)? {
            Ticket::Chunk(chunk) => chunk,
            Ticket::Stop => {
                shared.abort_upload();

                return Ok(());
            }
        };

        match read(reader, &mut chunk.data[..limit]) {
            Ok(0) => {
                shared.recycle(chunk);

                if let Some(left) = remaining.filter(|left| *left > 0) {
                    shared.cancel();

                    return Err(body_shorter_than_declared(left));
                }

                shared.end_upload();

                return Ok(());
            }
            Ok(read) => {
                chunk.used = read;

                if !shared.push_chunk(chunk) {
                    shared.abort_upload();

                    return Ok(());
                }

                if let Some(left) = remaining.as_mut() {
                    *left -= read as u64;
                }
            }
            Err(error) => {
                shared.recycle(chunk);
                shared.cancel();

                return Err(error);
            }
        }
    }

    let mut chunk = match shared.take_chunk(budget)? {
        Ticket::Chunk(chunk) => chunk,
        Ticket::Stop => {
            shared.abort_upload();

            return Ok(());
        }
    };
    let extended = read(reader, &mut chunk.data[..1]);

    shared.recycle(chunk);

    match extended {
        Ok(0) => {
            shared.end_upload();

            Ok(())
        }
        Ok(_) => {
            shared.cancel();

            Err(body_longer_than_declared())
        }
        Err(error) => {
            shared.cancel();

            Err(error)
        }
    }
}

fn read(reader: &mut dyn Read, buf: &mut [u8]) -> io::Result<usize> {
    loop {
        match reader.read(buf) {
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            other => return other,
        }
    }
}

fn body_shorter_than_declared(missing: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("request body ended {missing} bytes before the declared content-length"),
    )
}

fn body_longer_than_declared() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "request body exceeded the declared content-length",
    )
}
