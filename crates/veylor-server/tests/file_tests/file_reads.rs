use crate::file_fixture::*;
use std::fs;
#[cfg(target_os = "linux")]
use std::io::{Read, Seek, SeekFrom};
#[cfg(unix)]
use std::os::unix::fs::symlink;
use veylor_server::read_chunk;
#[cfg(target_os = "linux")]
use veylor_server::{FD_CACHE_ENTRIES, FdCache};

#[test]
fn read_chunk_truncation_surfaces_zero_before_plan_size() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/blob.bin")
        .expect("blob plan");
    fs::write(fixture.root.join("blob.bin"), b"short").expect("truncate");
    let mut out = [0u8; 8192];
    // NOTE: the pump treats a zero read before plan size as truncation, never success.
    assert_eq!(read_chunk(&plan, 0, &mut out).expect("short read"), 5);
    assert_eq!(read_chunk(&plan, 5, &mut out).expect("truncated read"), 0);
    fs::remove_file(fixture.root.join("blob.bin")).expect("delete");
    assert!(read_chunk(&plan, 0, &mut out).is_err());
}

#[test]
fn read_chunk_clamps_overgrown_file_to_plan_size() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    let mut grown = b"<html>hello</html>".to_vec();
    grown.extend_from_slice(&[b'g'; 100]);
    fs::write(fixture.root.join("index.html"), &grown).expect("grow");
    let mut out = vec![0u8; 65536];
    // NOTE: over-long reads clamp to the plan remainder; bytes past plan.size never serve.
    let got = read_chunk(&plan, 0, &mut out).expect("clamped read");
    assert_eq!(got, plan.size as usize);
    assert_eq!(&out[..got], b"<html>hello</html>");
}

#[test]
fn read_chunk_swapped_leaf_dir_fails_closed() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let victim = fixture.root.join("victim_dir.txt");
    fs::write(&victim, b"victim-bytes-1234").expect("victim");
    let plan = mounts
        .resolve(b"GET", b"/assets/victim_dir.txt")
        .expect("victim plan");
    fs::remove_file(&victim).expect("remove victim");
    fs::create_dir(&victim).expect("swap dir");
    let mut out = [0u8; 64];
    // NOTE: deterministic TOCTOU stage: resolve, swap on disk, then read on the stale plan.
    let error = read_chunk(&plan, 0, &mut out).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[cfg(unix)]
#[test]
fn read_chunk_swapped_leaf_symlink_fails_closed() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let victim = fixture.root.join("victim_link.txt");
    fs::write(&victim, b"victim-bytes-1234").expect("victim");
    let decoy = fixture.root.join("decoy.txt");
    fs::write(&decoy, b"decoy--bytes-1234").expect("decoy");
    assert_eq!(
        fs::metadata(&decoy).expect("decoy stat").len(),
        fs::metadata(&victim).expect("victim stat").len(),
        "same size pins the identity check, not the length"
    );
    let plan = mounts
        .resolve(b"GET", b"/assets/victim_link.txt")
        .expect("victim plan");
    fs::rename(&victim, fixture.root.join("victim_backup.txt")).expect("move aside");
    symlink(&decoy, &victim).expect("swap link");
    let mut out = [0u8; 64];
    // NOTE: deterministic TOCTOU stage: resolve, swap on disk, then read on the stale plan.
    let error = read_chunk(&plan, 0, &mut out).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
}

#[test]
fn read_chunk_swapped_leaf_fifo_fails_closed() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let victim = fixture.root.join("victim_fifo.txt");
    fs::write(&victim, b"victim-bytes-1234").expect("victim");
    let plan = mounts
        .resolve(b"GET", b"/assets/victim_fifo.txt")
        .expect("victim plan");
    fs::remove_file(&victim).expect("remove victim");
    assert!(
        std::process::Command::new("mkfifo")
            .arg(&victim)
            .status()
            .expect("spawn mkfifo")
            .success(),
        "mkfifo stages the fifo leaf"
    );
    // NOTE: the writer rendezvous lets the read-side open return so the fstat check runs; without it the open blocks.
    let path = victim.clone();
    let writer = std::thread::spawn(move || {
        let _ = std::fs::File::create(path);
    });
    let mut out = [0u8; 64];
    // NOTE: deterministic TOCTOU stage: resolve, swap on disk, then read on the stale plan.
    let error = read_chunk(&plan, 0, &mut out).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    writer.join().expect("writer joins");
}

#[cfg(target_os = "linux")]
fn cache_read_all(cache: &mut FdCache, plan: &veylor_server::FilePlan) -> Vec<u8> {
    let mut assembled = Vec::new();
    let mut offset = 0u64;
    while offset < plan.size {
        let file = cache.cached(plan).expect("cached open");
        let mut cursor: &std::fs::File = file;
        cursor.seek(SeekFrom::Start(offset)).expect("cache seek");
        let width = veylor_server::OUTPUT_CAP.min((plan.size - offset) as usize);
        let mut out = vec![0u8; width];
        let got = cursor.read(&mut out).expect("cache read");
        assert!(got > 0, "cache served no bytes before plan size");
        assembled.extend_from_slice(&out[..got]);
        offset += got as u64;
    }
    assembled
}

#[cfg(target_os = "linux")]
#[test]
fn fd_cache_opens_once_for_all_slices_and_reuses_across_transfers() {
    assert_eq!(FD_CACHE_ENTRIES, 1, "single-entry bound");
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/blob.bin")
        .expect("blob plan");
    let mut cache = FdCache::default();
    let first = cache_read_all(&mut cache, &plan);
    assert_eq!(first, fixture.blob);
    assert_eq!(
        cache.opens(),
        1,
        "one open for {} slices",
        plan.size / veylor_server::OUTPUT_CAP as u64
    );
    let second = cache_read_all(&mut cache, &plan);
    assert_eq!(second, fixture.blob);
    assert_eq!(cache.opens(), 1, "second transfer reuses the handle");
}

#[cfg(target_os = "linux")]
#[test]
fn fd_cache_revalidates_replaced_file_between_transfers() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    let mut cache = FdCache::default();
    assert_eq!(cache_read_all(&mut cache, &plan), b"<html>hello</html>");
    assert_eq!(cache.opens(), 1);
    let grown = b"<html>hello, replaced world</html>".to_vec();
    fs::write(fixture.root.join("index.html"), &grown).expect("replace");
    let fresh = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("fresh plan");
    assert_eq!(fresh.size, grown.len() as u64);
    assert_eq!(cache_read_all(&mut cache, &fresh), grown);
    assert_eq!(cache.opens(), 2, "size change reopens, no stale prefix");
}

#[cfg(target_os = "linux")]
#[test]
fn fd_cache_swapped_leaf_between_resolve_and_open_fails_closed() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let victim = fixture.root.join("victim_cache.txt");
    fs::write(&victim, b"victim-bytes-1234").expect("victim");
    let plan = mounts
        .resolve(b"GET", b"/assets/victim_cache.txt")
        .expect("victim plan");
    fs::remove_file(&victim).expect("remove victim");
    fs::write(&victim, b"strang-bytes-1234").expect("stranger");
    assert_eq!(
        fs::metadata(&victim).expect("stranger stat").len(),
        plan.size,
        "same size pins the identity check, not the length"
    );
    let mut cache = FdCache::default();
    // NOTE: deterministic TOCTOU stage: resolve, swap on disk, then cache on the stale plan.
    let error = cache.cached(&plan).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(cache.opens(), 0, "stranger never caches");
    let error = cache.cached(&plan).unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(cache.opens(), 0, "retry still fails closed");
}

#[cfg(target_os = "linux")]
#[test]
fn fd_cache_churn_across_more_files_than_entries_stays_exact() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let names = ["index.html", "sub/file.css", "app.js"];
    let bodies: Vec<Vec<u8>> = names
        .iter()
        .map(|name| fs::read(fixture.root.join(name)).expect("read body"))
        .collect();
    let mut cache = FdCache::default();
    for _ in 0..2 {
        for (name, body) in names.iter().zip(bodies.iter()) {
            let path = format!("/assets/{name}").into_bytes();
            let plan = mounts.resolve(b"GET", &path).expect("churn plan");
            assert_eq!(cache_read_all(&mut cache, &plan), *body);
        }
    }
    assert_eq!(
        cache.opens(),
        (names.len() * 2) as u64,
        "single entry misses on every switch"
    );
}
