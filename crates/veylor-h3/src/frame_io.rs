use quinn::{Connection, ReadExactError, RecvStream, VarInt};
use veylor_http3::frame;

pub(crate) async fn read_byte(recv: &mut RecvStream) -> Option<u8> {
    let mut byte = [0u8; 1];
    match recv.read_exact(&mut byte).await {
        Ok(()) => Some(byte[0]),
        Err(_) => None,
    }
}

pub(crate) async fn read_continuation(recv: &mut RecvStream) -> Option<u64> {
    let mut value = 0u64;
    loop {
        let byte = read_byte(recv).await?;
        value = value.checked_shl(7)?.checked_add(u64::from(byte & 0x7f))?;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
}

pub(crate) async fn read_varint(recv: &mut RecvStream) -> Result<Option<u64>, ()> {
    let mut first = [0u8; 1];
    match recv.read_exact(&mut first).await {
        Ok(()) => {}
        Err(ReadExactError::FinishedEarly(0)) => return Ok(None),
        Err(_) => return Err(()),
    }
    let length = 1usize << (first[0] >> 6);
    let mut bytes = [0u8; 8];
    bytes[0] = first[0];
    if length > 1 {
        recv.read_exact(&mut bytes[1..length])
            .await
            .map_err(|_| ())?;
    }
    match frame::decode_varint(&bytes[..length]) {
        Some((value, _)) => Ok(Some(value)),
        None => Err(()),
    }
}

pub(crate) async fn read_frame(
    recv: &mut RecvStream,
    limit: usize,
) -> Result<Option<(u64, Vec<u8>)>, ()> {
    let Some(kind) = read_varint(recv).await? else {
        return Ok(None);
    };
    let Some(length) = read_varint(recv).await? else {
        return Ok(None);
    };
    let length = usize::try_from(length).map_err(|_| ())?;
    if length > limit {
        return Err(());
    }
    let mut payload = vec![0u8; length];
    recv.read_exact(&mut payload).await.map_err(|_| ())?;
    Ok(Some((kind, payload)))
}

pub(crate) async fn discard(recv: &mut RecvStream, length: usize) -> Result<(), ()> {
    let mut scratch = [0u8; 1024];
    let mut remaining = length;
    while remaining > 0 {
        let take = remaining.min(scratch.len());
        recv.read_exact(&mut scratch[..take])
            .await
            .map_err(|_| ())?;
        remaining -= take;
    }
    Ok(())
}

pub(crate) fn close(connection: &Connection, code: u64, reason: &'static str) {
    if let Ok(code) = VarInt::from_u64(code) {
        connection.close(code, reason.as_bytes());
    }
}
