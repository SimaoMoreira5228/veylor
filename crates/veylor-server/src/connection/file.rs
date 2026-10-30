use crate::static_files::{FilePlan, read_chunk};

use super::{ConnClose, ConnState, OUTPUT_CAP};

// NOTE: single-entry fd cache (Linux); one handle per conn bounds fds, replace evicts.
#[cfg(target_os = "linux")]
pub const FD_CACHE_ENTRIES: usize = 1;

#[derive(Clone, Debug)]
pub(crate) struct FileTransfer {
    pub(crate) plan: FilePlan,
    pub(crate) offset: u64,
}

#[cfg(target_os = "linux")]
#[derive(Debug, Default)]
pub struct FdCache {
    dev: u64,
    ino: u64,
    size: u64,
    mtime: Option<std::time::SystemTime>,
    file: Option<std::fs::File>,
    opens: u64,
}

#[cfg(target_os = "linux")]
impl FdCache {
    // NOTE: TOCTOU honesty: plan-time validation stays the boundary; the fresh fd's dev+ino must match the plan's
    // resolve-time pair, so a stat/open swap fails closed (drop + error) instead of serving unapproved bytes. Hit path
    // serves the held fd on path-stat match alone; same-inode mutation keeps the clamp/short-read rules below.
    pub fn opens(&self) -> u64 {
        self.opens
    }
    pub fn cached(&mut self, plan: &FilePlan) -> std::io::Result<&std::fs::File> {
        let meta = std::fs::metadata(&plan.fs_path)?;
        let hit = self.file.is_some() && (self.dev, self.ino) == file_key(&meta);
        let hit = hit && self.size == meta.len() && self.mtime == meta.modified().ok();
        if !hit {
            let file = std::fs::File::open(&plan.fs_path)?;
            let fresh = file.metadata()?;
            if file_key(&fresh) != plan.identity() {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "file replaced underfoot",
                ));
            }
            (self.dev, self.ino) = file_key(&fresh);
            self.size = fresh.len();
            self.mtime = fresh.modified().ok();
            self.file = Some(file);
            self.opens += 1;
        }
        self.file
            .as_ref()
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::NotFound))
    }
}

#[cfg(target_os = "linux")]
fn file_key(meta: &std::fs::Metadata) -> (u64, u64) {
    let dev = std::os::unix::fs::MetadataExt::dev(meta);
    (dev, std::os::unix::fs::MetadataExt::ino(meta))
}

// NOTE: truncation rule: read_chunk clamps to the plan remainder, so over-long reads
// cannot happen; a zero-byte read before plan size means the file shrank underfoot and
// fails the transfer closed, never as a short success. Each stage re-opens the file
// through read_chunk; the retained-fd cache serves the sendfile path only. EPIPE and
// backpressure stay in the dispatch mapping, and Rust ignores SIGPIPE process-wide,
// so a closed receiver closes as PeerClosed, never a signal. Partial short reads
// advance the offset and resume on the next turn.
pub(crate) fn stage_file_chunk(conn: &mut ConnState) -> Result<(), ConnClose> {
    let Some(transfer) = conn.file.as_mut() else {
        return Err(ConnClose::TransportError);
    };
    if transfer.offset >= transfer.plan.size {
        return Ok(());
    }
    let width = (transfer.plan.size - transfer.offset).min(OUTPUT_CAP as u64) as usize;
    let Some(output) = conn.output.as_mut() else {
        return Err(ConnClose::TransportError);
    };
    match read_chunk(&transfer.plan, transfer.offset, &mut output[..width]) {
        Ok(0) => Err(ConnClose::TransportError),
        Ok(staged) => {
            transfer.offset += staged as u64;
            conn.out_len = staged;
            conn.out_sent = 0;
            conn.write_blocked = false;
            conn.read_ready = true;
            Ok(())
        }
        Err(_) => Err(ConnClose::TransportError),
    }
}

// NOTE: a shrink racing a send can zero-fill inside the plan bounds, where read_chunk would
// serve a short read instead. Either way the offset stalls below plan size and fails closed.
// Bytes past plan.size never serve (count clamp) and only this file's pages ever map.
#[cfg(target_os = "linux")]
pub(crate) fn sendfile_file_chunk(
    conn: &mut ConnState,
    byte_budget: u64,
) -> Result<u64, ConnClose> {
    let Some(transfer) = conn.file.as_ref() else {
        return Err(ConnClose::TransportError);
    };
    if transfer.offset >= transfer.plan.size {
        return Ok(0);
    }
    if transfer.plan.multipart {
        return stage_file_chunk(conn).map(|_| 0); // NOTE: framing interleaves file bytes
    }
    let rest = transfer.plan.size - transfer.offset;
    let width = std::cmp::min(std::cmp::min(rest, OUTPUT_CAP as u64), byte_budget) as usize;
    if width == 0 || conn.write_blocked {
        return Ok(0);
    }
    let file = conn
        .file_cache
        .cached(&transfer.plan)
        .map_err(|_| ConnClose::TransportError)?;
    let mut cursor = transfer.plan.start + transfer.offset;
    for _ in 0..2 {
        match rustix::fs::sendfile(&conn.stream, file, Some(&mut cursor), width) {
            Ok(sent) => {
                if sent == 0 {
                    return Err(ConnClose::TransportError);
                }
                let Some(transfer) = conn.file.as_mut() else {
                    return Err(ConnClose::TransportError);
                };
                transfer.offset = cursor - transfer.plan.start;
                return Ok(sent as u64);
            }
            Err(error) if error == rustix::io::Errno::INTR => continue,
            Err(error) if error == rustix::io::Errno::AGAIN => {
                conn.write_blocked = true;
                return Ok(0);
            }
            Err(error) if error == rustix::io::Errno::PIPE => return Err(ConnClose::PeerClosed),
            Err(error) if error == rustix::io::Errno::CONNRESET => return Err(ConnClose::Reset),
            Err(_) => return Err(ConnClose::TransportError),
        }
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use veylor_runtime::{ConnectionToken, TcpStream};

    use crate::static_files::FileMounts;

    use super::*;

    static NEXT_SHRINK_ID: AtomicU64 = AtomicU64::new(0);

    fn test_conn() -> ConnState {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind rig");
        let addr = listener.local_addr().expect("rig addr");
        let stream = TcpStream::connect(addr).expect("rig connect");

        ConnState::new(
            ConnectionToken {
                worker_id: 0,
                slot: 0,
                request_seq: 0,
                stream_id: 0,
                worker_generation: 1,
                generation: 1,
            },
            stream,
        )
    }

    #[test]
    fn staged_chunk_on_shrunk_file_fails_closed() {
        let id = NEXT_SHRINK_ID.fetch_add(1, Ordering::SeqCst);
        let dir =
            std::env::temp_dir().join(format!("veylor-file-shrink-{}-{id}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("fixture root");
        std::fs::write(dir.join("blob.bin"), vec![0xAB; 4096]).expect("fixture blob");

        let mut mounts = FileMounts::new();
        mounts.register(b"/assets/", &dir).expect("register mount");
        let plan = mounts
            .resolve(b"GET", b"/assets/blob.bin")
            .expect("plan fixture");

        std::fs::OpenOptions::new()
            .write(true)
            .open(dir.join("blob.bin"))
            .expect("open blob")
            .set_len(0)
            .expect("shrink blob");

        let mut conn = test_conn();
        ConnState::materialize_output(&mut conn.output);
        conn.file = Some(FileTransfer { plan, offset: 0 });

        match stage_file_chunk(&mut conn) {
            Err(ConnClose::TransportError) => {}
            Err(other) => panic!("shrink must close as transport error, got {other:?}"),
            Ok(()) => panic!("shrink must not stage success"),
        }

        std::fs::remove_dir_all(&dir).expect("scrub fixture");
    }
}
