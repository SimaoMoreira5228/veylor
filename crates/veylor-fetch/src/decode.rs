use crate::error::Result;

#[cfg(feature = "gzip")]
pub(crate) struct Gunzip {
    inner: flate2::write::GzDecoder<Vec<u8>>,
}

#[cfg(feature = "gzip")]
impl Gunzip {
    pub fn start(encoding: Option<&[u8]>, skip: bool) -> Option<Self> {
        let gzip = encoding.is_some_and(|value| {
            value.trim_ascii().eq_ignore_ascii_case(b"gzip")
                || value.trim_ascii().eq_ignore_ascii_case(b"x-gzip")
        });

        if skip || !gzip {
            return None;
        }

        Some(Self {
            inner: flate2::write::GzDecoder::new(Vec::new()),
        })
    }

    pub fn feed(&mut self, raw: &[u8]) -> Result<()> {
        use std::io::Write;

        self.inner.get_mut().clear();
        self.inner
            .write_all(raw)
            .map_err(|e| crate::error::Error::Decode(e.to_string()))
    }

    pub fn finish(&mut self) -> Result<()> {
        self.inner.get_mut().clear();
        self.inner
            .try_finish()
            .map_err(|e| crate::error::Error::Decode(e.to_string()))
    }

    pub fn output(&self) -> &[u8] {
        self.inner.get_ref()
    }
}

#[cfg(not(feature = "gzip"))]
pub(crate) enum Gunzip {}

#[cfg(not(feature = "gzip"))]
impl Gunzip {
    pub fn start(_encoding: Option<&[u8]>, _skip: bool) -> Option<Self> {
        None
    }

    pub fn feed(&mut self, _raw: &[u8]) -> Result<()> {
        match *self {}
    }

    pub fn finish(&mut self) -> Result<()> {
        match *self {}
    }

    pub fn output(&self) -> &[u8] {
        match *self {}
    }
}
