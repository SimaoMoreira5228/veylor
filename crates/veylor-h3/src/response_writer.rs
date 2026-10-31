use std::io;

use http::StatusCode;
use quinn::{SendStream, VarInt};
use veylor_http3::{frame, qpack};

pub struct ResponseWriter<'a> {
    send: &'a mut SendStream,
    headers_sent: bool,
    finished: bool,
}

impl<'a> ResponseWriter<'a> {
    pub(crate) fn new(send: &'a mut SendStream) -> Self {
        Self {
            send,
            headers_sent: false,
            finished: false,
        }
    }

    pub async fn send_headers(
        &mut self,
        status: StatusCode,
        headers: &[(Vec<u8>, Vec<u8>)],
    ) -> io::Result<()> {
        if self.headers_sent {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "headers already sent",
            ));
        }
        let status_text = status.as_u16().to_string();
        let fields = encode_fields(status_text.as_bytes(), headers);
        self.write_block(frame::HEADERS, &fields).await?;
        self.headers_sent = true;
        Ok(())
    }

    pub async fn write(&mut self, data: &[u8]) -> io::Result<()> {
        if !self.headers_sent || self.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream is not writable",
            ));
        }
        if data.is_empty() {
            return Ok(());
        }
        let mut head = [0u8; 16];
        let used = frame::encode_frame_header(frame::DATA, data.len() as u64, &mut head);
        self.send
            .write_all(&head[..used])
            .await
            .map_err(io::Error::other)?;
        self.send.write_all(data).await.map_err(io::Error::other)
    }

    pub async fn write_trailers(&mut self, trailers: &[(Vec<u8>, Vec<u8>)]) -> io::Result<()> {
        if !self.headers_sent || self.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "stream is not writable",
            ));
        }
        let fields = encode_fields(b"", trailers);
        self.write_block(frame::HEADERS, &fields).await
    }

    pub async fn finish(&mut self) -> io::Result<()> {
        if self.finished {
            return Ok(());
        }
        self.finished = true;
        self.send.finish().map_err(io::Error::other)
    }

    pub fn reset(&mut self, code: u64) {
        if let Ok(code) = VarInt::from_u64(code) {
            let _ = self.send.reset(code);
        }
    }

    async fn write_block(&mut self, kind: u64, fields: &[(&[u8], &[u8])]) -> io::Result<()> {
        let block = qpack::encode(fields);
        let mut encoded = Vec::with_capacity(block.len() + 16);
        frame::encode_frame(kind, &block, &mut encoded);
        self.send
            .write_all(&encoded)
            .await
            .map_err(io::Error::other)
    }
}

fn encode_fields<'a>(
    status: &'a [u8],
    headers: &'a [(Vec<u8>, Vec<u8>)],
) -> Vec<(&'a [u8], &'a [u8])> {
    let mut fields = Vec::with_capacity(headers.len() + 2);
    if !status.is_empty() {
        fields.push((b":status".as_slice(), status));
    }
    for (name, value) in headers {
        if prohibited(name) {
            continue;
        }
        fields.push((name.as_slice(), value.as_slice()));
    }
    fields
}

fn prohibited(name: &[u8]) -> bool {
    [
        b"connection".as_slice(),
        b"keep-alive",
        b"proxy-connection",
        b"transfer-encoding",
        b"upgrade",
        b":status",
    ]
    .iter()
    .any(|banned| name.eq_ignore_ascii_case(banned))
}
