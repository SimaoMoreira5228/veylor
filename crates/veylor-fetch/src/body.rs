#[derive(Clone, Debug, Default)]
pub enum Body {
    #[default]
    Empty,
    Static(&'static [u8]),
    Owned(Vec<u8>),
}

impl Body {
    pub fn as_slice(&self) -> &[u8] {
        match self {
            Self::Empty => &[],
            Self::Static(bytes) => bytes,
            Self::Owned(bytes) => bytes,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }
}

impl From<Vec<u8>> for Body {
    fn from(bytes: Vec<u8>) -> Self {
        Self::Owned(bytes)
    }
}

impl From<&'static [u8]> for Body {
    fn from(bytes: &'static [u8]) -> Self {
        Self::Static(bytes)
    }
}

impl From<String> for Body {
    fn from(text: String) -> Self {
        Self::Owned(text.into_bytes())
    }
}

impl From<&'static str> for Body {
    fn from(text: &'static str) -> Self {
        Self::Static(text.as_bytes())
    }
}
