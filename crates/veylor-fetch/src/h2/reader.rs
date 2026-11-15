use std::io::{self, Read};
use std::net::TcpStream;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::shared::{DriverShared, Failure, STATE_CLOSED};

const READ_CHUNK: usize = 16 * 1024 + 512;
const MAX_INBOUND: usize = 2 * 1024 * 1024;

// NOTE: this thread only moves ciphertext from the socket into the driver's inbox. The blocking
// read is what lets the driver sleep on a condvar and still notice the peer instantly.
pub(super) fn run(mut tcp: TcpStream, shared: Arc<DriverShared>) {
    let mut chunk = vec![0u8; READ_CHUNK];

    // NOTE: the handshake leaves the connect timeout armed on the socket. Every h2 deadline is
    // judged by the driver, so a socket read timeout here would kill a connection the pool is
    // deliberately holding open.
    if let Err(error) = tcp.set_read_timeout(None) {
        shared.core.lock().eof = Some(Failure::new(error.kind(), error.to_string()));
        shared.wake.notify_one();

        return;
    }

    let failure = loop {
        let read = match tcp.read(&mut chunk) {
            Ok(0) => break Failure::new(io::ErrorKind::UnexpectedEof, "h2 connection closed"),
            Ok(read) => read,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => break Failure::new(error.kind(), error.to_string()),
        };

        let mut core = shared.core.lock();

        while core.inbound.len() >= MAX_INBOUND && !closed(&shared) {
            shared.room.wait(&mut core);
        }

        if closed(&shared) {
            return;
        }

        core.inbound.extend_from_slice(&chunk[..read]);
        drop(core);
        shared.wake.notify_one();
    };

    shared.core.lock().eof = Some(failure);
    shared.wake.notify_one();
}

fn closed(shared: &DriverShared) -> bool {
    shared.state.load(Ordering::Acquire) == STATE_CLOSED
}
