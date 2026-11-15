const DEFAULT_LIMIT: usize = 10;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Policy {
    limit: usize,
}

impl Policy {
    pub fn limited(max: usize) -> Self {
        Self { limit: max }
    }

    pub fn none() -> Self {
        Self { limit: 0 }
    }

    pub fn limit(&self) -> usize {
        self.limit
    }
}

impl Default for Policy {
    fn default() -> Self {
        Self::limited(DEFAULT_LIMIT)
    }
}
