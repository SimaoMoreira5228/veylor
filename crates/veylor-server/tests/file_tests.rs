use std::fs;
use std::os::unix::fs::symlink;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use veylor_server::{FileMounts, FileReject, MountError, read_chunk};

static NEXT_ID: AtomicU64 = AtomicU64::new(0);
const BLOB_LEN: usize = 1 << 20;

struct Fixture {
    root: PathBuf,
    blob: Vec<u8>,
}

impl Fixture {
    fn build() -> Self {
        let id = NEXT_ID.fetch_add(1, Ordering::SeqCst);
        let root =
            std::env::temp_dir().join(format!("veylor-file-test-{}-{id}", std::process::id()));
        fs::create_dir_all(root.join("sub")).expect("fixture dirs");
        fs::create_dir_all(root.join("inner_root")).expect("inner root");
        fs::write(root.join("index.html"), b"<html>hello</html>").expect("index");
        fs::write(root.join("sub").join("file.css"), b"body{color:red}").expect("css");
        fs::write(root.join("empty.txt"), b"").expect("empty");
        fs::write(root.join("app.js"), b"console.log(1);").expect("js");
        fs::write(root.join("data.json"), b"{\"a\":1}").expect("json");
        fs::write(root.join("pic.png"), b"\x89PNG....").expect("png");
        fs::write(root.join("photo.jpg"), b"\xff\xd8\xff...").expect("jpg");
        fs::write(root.join("img.svg"), b"<svg/>").expect("svg");
        fs::write(root.join("odd.xyz"), b"odd").expect("unknown ext");
        fs::write(root.join("noext"), b"noext").expect("no ext");
        fs::write(root.join("inner_root").join("deep.txt"), b"deep").expect("deep");
        let mut blob = Vec::with_capacity(BLOB_LEN);
        for i in 0..BLOB_LEN {
            blob.push((i % 251) as u8);
        }
        fs::write(root.join("blob.bin"), &blob).expect("blob");
        Self { root, blob }
    }

    fn mounts(&self) -> FileMounts {
        let mut mounts = FileMounts::new();
        mounts
            .register(b"/assets/", &self.root)
            .expect("register assets");
        mounts
            .register(b"/assets/inner/", &self.root.join("inner_root"))
            .expect("register inner");
        assert!(!mounts.is_frozen());
        mounts.freeze();
        assert!(mounts.is_frozen());
        mounts
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.root);
    }
}

fn forbidden_cases() -> Vec<Vec<u8>> {
    vec![
        b"/assets/../secret".to_vec(),
        b"/assets/%2e%2e/secret".to_vec(),
        b"/assets/%2E%2E/secret".to_vec(),
        b"/assets/..%2fsecret".to_vec(),
        b"/assets/%2e/secret".to_vec(),
        b"/assets/sub/../../secret".to_vec(),
        b"/assets//index.html".to_vec(),
        b"/assets/sub/".to_vec(),
        b"/assets/%2findex.html".to_vec(),
        b"/assets/%5cindex.html".to_vec(),
        b"/assets/%00".to_vec(),
        b"/assets/%zz".to_vec(),
        b"/assets/%2".to_vec(),
        b"/assets/%".to_vec(),
        b"/assets/a\\b".to_vec(),
        b"/assets/./index.html".to_vec(),
        b"/assets/.".to_vec(),
        b"/assets/index.html?x=1".to_vec(),
        b"/assets/index.html#frag".to_vec(),
    ]
}

#[test]
fn get_plan_carries_exact_size_and_type() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("get plan");
    assert_eq!(plan.size, b"<html>hello</html>".len() as u64);
    assert_eq!(plan.content_type, "text/html");
    assert!(!plan.head_only);
    assert_eq!(plan.fs_path, fixture.root.join("index.html"));

    let css = mounts
        .resolve(b"GET", b"/assets/sub/file.css")
        .expect("css plan");
    assert_eq!(css.size, b"body{color:red}".len() as u64);
    assert_eq!(css.content_type, "text/css");

    let blob = mounts
        .resolve(b"GET", b"/assets/blob.bin")
        .expect("blob plan");
    assert_eq!(blob.size, BLOB_LEN as u64);
    assert_eq!(blob.content_type, "application/octet-stream");
}

#[test]
fn head_plan_matches_get_headers() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let get = mounts.resolve(b"GET", b"/assets/index.html").expect("get");
    let head = mounts
        .resolve(b"HEAD", b"/assets/index.html")
        .expect("head");
    assert!(head.head_only);
    assert_eq!(head.size, get.size);
    assert_eq!(head.content_type, get.content_type);
    assert_eq!(head.fs_path, get.fs_path);
}

#[test]
fn content_type_table_covers_named_types() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let table = [
        ("app.js", "text/javascript"),
        ("data.json", "application/json"),
        ("pic.png", "image/png"),
        ("photo.jpg", "image/jpeg"),
        ("img.svg", "image/svg+xml"),
        ("empty.txt", "text/plain"),
        ("odd.xyz", "application/octet-stream"),
        ("noext", "application/octet-stream"),
        ("blob.bin", "application/octet-stream"),
    ];
    for (name, expected) in table {
        let path = format!("/assets/{name}").into_bytes();
        let plan = mounts.resolve(b"GET", &path).expect("typed plan");
        assert_eq!(plan.content_type, expected, "type for {name}");
    }
}

#[test]
fn traversal_battery_rejects_forbidden() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    for path in forbidden_cases() {
        let result = mounts.resolve(b"GET", &path);
        assert_eq!(
            result.err(),
            Some(FileReject::Forbidden),
            "path {}",
            String::from_utf8_lossy(&path)
        );
    }
}

#[test]
fn missing_and_unknown_prefix_read_not_found() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/missing.html").err(),
        Some(FileReject::NotFound)
    );
    assert_eq!(
        mounts.resolve(b"GET", b"/other/index.html").err(),
        Some(FileReject::NotFound)
    );
    assert_eq!(
        mounts.resolve(b"GET", b"assets/index.html").err(),
        Some(FileReject::NotFound)
    );
    assert_eq!(
        mounts.resolve(b"POST", b"/assets/index.html").err(),
        Some(FileReject::NotFound)
    );
    assert_eq!(
        mounts.resolve(b"DELETE", b"/assets/index.html").err(),
        Some(FileReject::NotFound)
    );
    // NOTE: trailing dots are legal names here (only `.` and `..` reject, mirroring Odin); absent files read as NotFound.
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/trailing.").err(),
        Some(FileReject::NotFound)
    );
}

#[test]
fn directories_reject_not_a_file() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/").err(),
        Some(FileReject::NotAFile)
    );
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/sub").err(),
        Some(FileReject::NotAFile)
    );
}

#[test]
fn symlinks_reject_in_every_component() {
    let fixture = Fixture::build();
    symlink(
        fixture.root.join("index.html"),
        fixture.root.join("link.html"),
    )
    .expect("file link");
    symlink(fixture.root.join("sub"), fixture.root.join("linkdir")).expect("dir link");
    symlink(fixture.root.join("loop"), fixture.root.join("loop")).expect("self loop");
    let mounts = fixture.mounts();
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/link.html").err(),
        Some(FileReject::Forbidden)
    );
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/linkdir/file.css").err(),
        Some(FileReject::Forbidden)
    );
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/loop").err(),
        Some(FileReject::Forbidden)
    );
    assert_eq!(
        mounts.resolve(b"GET", b"/assets/loop/x").err(),
        Some(FileReject::Forbidden)
    );
}

#[test]
fn empty_file_plans_zero_length() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/empty.txt")
        .expect("empty plan");
    assert_eq!(plan.size, 0);
    assert_eq!(plan.content_type, "text/plain");
    let mut out = [0xAA; 16];
    assert_eq!(read_chunk(&plan, 0, &mut out).expect("empty read"), 0);
    assert_eq!(
        read_chunk(&plan, 1, &mut out).unwrap_err().kind(),
        std::io::ErrorKind::InvalidInput
    );
}

#[test]
fn read_chunk_resume_assembles_exact_bytes() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/blob.bin")
        .expect("blob plan");
    let mut assembled = Vec::new();
    let mut offset = 0u64;
    for width in [1u64, 7, 100_000, 3, 65536, 999_983] {
        let end = (offset + width).min(plan.size);
        let mut out = vec![0u8; (end - offset) as usize];
        let got = read_chunk(&plan, offset, &mut out).expect("chunk read");
        assert_eq!(got, out.len());
        assembled.extend_from_slice(&out[..got]);
        offset = end;
    }
    while offset < plan.size {
        let mut out = vec![0u8; 8192];
        let got = read_chunk(&plan, offset, &mut out).expect("tail read");
        assert!(got > 0);
        assembled.extend_from_slice(&out[..got]);
        offset += got as u64;
    }
    assert_eq!(assembled, fixture.blob);

    let mut out = [0u8; 8];
    assert_eq!(
        read_chunk(&plan, plan.size, &mut out).expect("read at end"),
        0
    );
    assert_eq!(read_chunk(&plan, 0, &mut []).expect("empty out"), 0);
    assert_eq!(
        read_chunk(&plan, plan.size + 1, &mut out)
            .unwrap_err()
            .kind(),
        std::io::ErrorKind::InvalidInput
    );
    assert!(read_chunk(&plan, plan.size + 1, &mut []).is_err());
}

#[test]
fn longest_prefix_mount_wins() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/inner/deep.txt")
        .expect("inner plan");
    assert_eq!(
        plan.fs_path,
        fixture.root.join("inner_root").join("deep.txt")
    );
    assert_eq!(plan.content_type, "text/plain");
    let outer = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("outer plan");
    assert_eq!(outer.fs_path, fixture.root.join("index.html"));
}

#[test]
fn duplicate_and_frozen_mounts_reject() {
    let fixture = Fixture::build();
    let mut mounts = FileMounts::new();
    mounts.register(b"/assets/", &fixture.root).expect("first");
    assert_eq!(
        mounts.register(b"/assets/", &fixture.root).err(),
        Some(MountError::Duplicate)
    );
    mounts.freeze();
    assert_eq!(
        mounts.register(b"/late/", &fixture.root).err(),
        Some(MountError::Frozen)
    );
}

#[test]
fn mount_table_full_rejects() {
    let fixture = Fixture::build();
    let mut mounts = FileMounts::new();
    for i in 0..veylor_server::MAX_MOUNTS {
        let prefix = format!("/m{i}/").into_bytes();
        mounts.register(&prefix, &fixture.root).expect("fill");
    }
    assert_eq!(
        mounts.register(b"/overflow/", &fixture.root).err(),
        Some(MountError::Full)
    );
}

#[test]
fn invalid_mounts_reject() {
    let fixture = Fixture::build();
    let mut mounts = FileMounts::new();
    for prefix in [
        b"assets/".as_slice(),
        b"/assets".as_slice(),
        b"".as_slice(),
        b"/../".as_slice(),
        b"/./".as_slice(),
        b"/a?/".as_slice(),
        b"/a#/".as_slice(),
        b"/a%2f/".as_slice(),
        b"//".as_slice(),
    ] {
        assert_eq!(
            mounts.register(prefix, &fixture.root).err(),
            Some(MountError::Invalid),
            "prefix {}",
            String::from_utf8_lossy(prefix)
        );
    }
    assert_eq!(
        mounts
            .register(b"/assets/", Path::new("relative/root"))
            .err(),
        Some(MountError::Invalid)
    );
    assert_eq!(
        mounts
            .register(b"/assets/", &fixture.root.join("missing"))
            .err(),
        Some(MountError::Invalid)
    );
    assert_eq!(
        mounts
            .register(b"/assets/", &fixture.root.join("index.html"))
            .err(),
        Some(MountError::Invalid)
    );
}

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
