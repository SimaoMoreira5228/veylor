use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use veylor_server::FileMounts;

pub(crate) static NEXT_ID: AtomicU64 = AtomicU64::new(0);

pub(crate) const BLOB_LEN: usize = 1 << 20;

pub(crate) struct Fixture {
    pub(crate) root: PathBuf,
    pub(crate) blob: Vec<u8>,
}

impl Fixture {
    pub(crate) fn build() -> Self {
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

    pub(crate) fn mounts(&self) -> FileMounts {
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
