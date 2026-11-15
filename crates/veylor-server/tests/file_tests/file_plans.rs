use crate::file_fixture::*;
use std::fs;
#[cfg(unix)]
use std::os::unix::fs::symlink;
use std::path::Path;
use veylor_server::{
    FileMounts, FileReject, MountError, parse_http_date, read_chunk, write_http_date,
};

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

#[cfg(unix)]
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

fn emit(secs: u64) -> String {
    let mut out = [0u8; 64];
    let len = write_http_date(secs, &mut out);
    String::from_utf8(out[..len].to_vec()).expect("date ascii")
}

#[test]
fn http_date_known_vectors_emit_exact_imf() {
    for (secs, expected) in [
        (0, "Thu, 01 Jan 1970 00:00:00 GMT"),
        (784_111_777, "Sun, 06 Nov 1994 08:49:37 GMT"),
        (946_684_800, "Sat, 01 Jan 2000 00:00:00 GMT"),
        (951_782_400, "Tue, 29 Feb 2000 00:00:00 GMT"),
        (4_102_444_800, "Fri, 01 Jan 2100 00:00:00 GMT"),
    ] {
        assert_eq!(emit(secs), expected, "emit for {secs}");
        assert_eq!(
            parse_http_date(expected.as_bytes()),
            Some(secs),
            "parse {expected}"
        );
    }
}

#[test]
fn http_date_obsolete_formats_parse_to_same_instant() {
    assert_eq!(
        parse_http_date(b"Sunday, 06-Nov-94 08:49:37 GMT"),
        Some(784_111_777)
    );
    assert_eq!(
        parse_http_date(b"Sun Nov  6 08:49:37 1994"),
        Some(784_111_777)
    );
    assert_eq!(
        parse_http_date(b"Sun Nov 16 08:49:37 1994"),
        parse_http_date(b"Sun, 16 Nov 1994 08:49:37 GMT")
    );
}

#[test]
fn http_date_emit_parse_round_trips_every_magnitude() {
    for secs in [
        0,
        1,
        86_399,
        784_111_777,
        951_782_400,
        4_102_444_800,
        u64::from(u32::MAX),
    ] {
        let text = emit(secs);
        assert_eq!(
            parse_http_date(text.as_bytes()),
            Some(secs),
            "round trip {secs}"
        );
    }
}

#[test]
fn http_date_malformed_values_reject_to_none() {
    for value in [
        "",
        "yesterday",
        "Sun, 06 Nov 1994 08:49:37",
        "Sun, 06 Nov 1994 08:49:37 UTC",
        "Sun, 32 Nov 1994 08:49:37 GMT",
        "Sun, 00 Nov 1994 08:49:37 GMT",
        "Sun, 06 Xyz 1994 08:49:37 GMT",
        "Sun, 06 Nov 1994 25:49:37 GMT",
        "Sun, 06 Nov 1994 08:61:37 GMT",
        "Sun, 29 Feb 1999 00:00:00 GMT",
        "Sun, 29 Feb 1900 00:00:00 GMT",
        "Sun, 06 Nov 1994 08:49:37 GMT ",
        "Funday, 06-Nov-94 08:49:37 GMT",
        "Sun Nov  6 08:49:37 94",
    ] {
        assert_eq!(parse_http_date(value.as_bytes()), None, "reject {value:?}");
    }
    assert!(
        parse_http_date(b"Sun, 06 Nov 1994 08:49:60 GMT").is_some(),
        "leap second folds"
    );
}

#[test]
fn plan_carries_truncated_mtime_from_same_stat() {
    let fixture = Fixture::build();
    let mounts = fixture.mounts();
    let plan = mounts
        .resolve(b"GET", b"/assets/index.html")
        .expect("index plan");
    let meta = fs::metadata(fixture.root.join("index.html")).expect("stat");
    let expected = meta
        .modified()
        .expect("mtime")
        .duration_since(std::time::UNIX_EPOCH)
        .expect("post-epoch")
        .as_secs();
    assert_eq!(plan.mtime, expected);
    let head = mounts
        .resolve(b"HEAD", b"/assets/index.html")
        .expect("head plan");
    assert_eq!(head.mtime, plan.mtime);
}
