use std::path::{Path, PathBuf};

use super::{FilePlan, MAX_FILE_SIZE, file_key, system_time_secs};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ContentCoding {
    Br,
    Gzip,
}

impl ContentCoding {
    pub fn as_bytes(self) -> &'static [u8] {
        match self {
            Self::Br => b"br",
            Self::Gzip => b"gzip",
        }
    }

    fn suffix(self) -> &'static str {
        match self {
            Self::Br => ".br",
            Self::Gzip => ".gz",
        }
    }
}

pub fn select_encoding(
    mut plan: FilePlan,
    accept: Option<&[u8]>,
    range: Option<&[u8]>,
) -> FilePlan {
    if range.is_some() {
        return plan;
    }
    for coding in accepted_codings(accept).into_iter().flatten() {
        let Some(twin) = twin(&plan.fs_path, coding) else {
            continue;
        };
        plan.fs_path = twin.path;
        plan.size = twin.size;
        plan.total = twin.size;
        plan.mtime = twin.mtime;
        plan.dev = twin.dev;
        plan.ino = twin.ino;
        plan.content_encoding = Some(coding);
        return plan;
    }
    plan
}

fn accepted_codings(accept: Option<&[u8]>) -> [Option<ContentCoding>; 2] {
    let Some(value) = accept else {
        return [None, None];
    };

    let mut br = None;
    let mut gzip = None;
    let mut star = None;
    for item in value.split(|byte| *byte == b',') {
        let item = item.trim_ascii();
        if item.is_empty() {
            continue;
        }

        let mut parts = item.split(|byte| *byte == b';');
        let token = parts.next().unwrap_or_default().trim_ascii();
        let mut quality = 1.0f32;
        for parameter in parts {
            let parameter = parameter.trim_ascii();
            let Some(equals) = parameter.iter().position(|byte| *byte == b'=') else {
                continue;
            };
            if !parameter[..equals].trim_ascii().eq_ignore_ascii_case(b"q") {
                continue;
            }
            quality = std::str::from_utf8(parameter[equals + 1..].trim_ascii())
                .ok()
                .and_then(|value| value.parse::<f32>().ok())
                .unwrap_or(0.0)
                .clamp(0.0, 1.0);
        }

        if token.eq_ignore_ascii_case(b"br") {
            br = Some(quality);
        } else if token.eq_ignore_ascii_case(b"gzip") || token.eq_ignore_ascii_case(b"x-gzip") {
            gzip = Some(quality);
        } else if token == b"*" {
            star = Some(quality);
        }
    }

    let br = br.or(star);
    let gzip = gzip.or(star);
    let mut candidates = [(None, 0.0f32); 2];
    let mut count = 0;
    if let Some(quality) = br.filter(|quality| *quality > 0.0) {
        candidates[count] = (Some(ContentCoding::Br), quality);
        count += 1;
    }
    if let Some(quality) = gzip.filter(|quality| *quality > 0.0) {
        candidates[count] = (Some(ContentCoding::Gzip), quality);
        count += 1;
    }
    if count == 2 && candidates[1].1 > candidates[0].1 {
        candidates.swap(0, 1);
    }

    [candidates[0].0, candidates[1].0]
}

struct Twin {
    path: PathBuf,
    size: u64,
    mtime: u64,
    dev: u64,
    ino: u64,
}

fn twin(original: &Path, coding: ContentCoding) -> Option<Twin> {
    let mut name = original.as_os_str().to_owned();
    name.push(coding.suffix());
    let path = PathBuf::from(name);

    let meta = std::fs::symlink_metadata(&path).ok()?;
    if meta.file_type().is_symlink() || !meta.file_type().is_file() {
        return None;
    }
    let size = meta.len();
    if size > MAX_FILE_SIZE {
        return None;
    }
    let (dev, ino) = file_key(&meta);

    Some(Twin {
        path,
        size,
        mtime: system_time_secs(meta.modified().ok()),
        dev,
        ino,
    })
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    use crate::static_files::FileMounts;

    use super::*;

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let id = NEXT.fetch_add(1, Ordering::SeqCst);
            let root =
                std::env::temp_dir().join(format!("veylor-encoding-{}-{id}", std::process::id()));
            std::fs::create_dir_all(&root).expect("root");
            std::fs::write(root.join("app.js"), b"identity").expect("identity");
            std::fs::write(root.join("app.js.br"), b"brotli").expect("br");
            std::fs::write(root.join("app.js.gz"), b"gzip").expect("gz");
            Self { root }
        }

        fn plan_with_range(&self, range: Option<&[u8]>) -> FilePlan {
            let mut mounts = FileMounts::new();
            mounts.register(b"/assets/", &self.root).expect("mount");
            let plan = mounts.resolve(b"GET", b"/assets/app.js").expect("plan");
            select_encoding(plan, Some(b"br, gzip"), range)
        }

        fn plan(&self, accept: Option<&[u8]>) -> FilePlan {
            let mut mounts = FileMounts::new();
            mounts.register(b"/assets/", &self.root).expect("mount");
            let plan = mounts.resolve(b"GET", b"/assets/app.js").expect("plan");
            select_encoding(plan, accept, None)
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            std::fs::remove_dir_all(&self.root).expect("cleanup");
        }
    }

    #[test]
    fn picks_best_accepted_coding() {
        let fixture = Fixture::new();

        let plan = fixture.plan(Some(b"gzip, br"));
        assert_eq!(plan.content_encoding, Some(ContentCoding::Br));
        assert_eq!(plan.size, 6);

        let plan = fixture.plan(Some(b"gzip"));
        assert_eq!(plan.content_encoding, Some(ContentCoding::Gzip));

        let plan = fixture.plan(Some(b"gzip;q=0.2, br;q=0.9"));
        assert_eq!(plan.content_encoding, Some(ContentCoding::Br));

        let plan = fixture.plan(Some(b"br;q=0, gzip"));
        assert_eq!(plan.content_encoding, Some(ContentCoding::Gzip));

        let plan = fixture.plan(Some(b"br;q=0, gzip;q=0"));
        assert_eq!(plan.content_encoding, None);

        let plan = fixture.plan(Some(b"*"));
        assert_eq!(plan.content_encoding, Some(ContentCoding::Br));

        let plan = fixture.plan(None);
        assert_eq!(plan.content_encoding, None);
        assert_eq!(plan.size, 8);
    }

    #[test]
    fn range_keeps_identity() {
        let fixture = Fixture::new();
        let plan = fixture.plan_with_range(Some(b"bytes=0-1"));
        assert_eq!(plan.content_encoding, None);
        assert_eq!(plan.fs_path, fixture.root.join("app.js"));
    }

    #[test]
    fn missing_twin_falls_back() {
        let fixture = Fixture::new();
        std::fs::remove_file(fixture.root.join("app.js.br")).expect("remove br");
        std::fs::remove_file(fixture.root.join("app.js.gz")).expect("remove gz");

        let plan = fixture.plan(Some(b"br, gzip"));
        assert_eq!(plan.content_encoding, None);
    }

    #[test]
    fn symlinked_twin_is_ignored() {
        let fixture = Fixture::new();
        std::fs::remove_file(fixture.root.join("app.js.br")).expect("remove br");
        #[cfg(unix)]
        std::os::unix::fs::symlink(fixture.root.join("app.js"), fixture.root.join("app.js.br"))
            .expect("symlink");

        let plan = fixture.plan(Some(b"br"));
        assert_eq!(plan.content_encoding, None);
    }
}
