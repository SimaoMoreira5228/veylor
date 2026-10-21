pub(super) struct HeadersAssembler<'a> {
    pub(super) buffer: &'a mut [u8],
    pub(super) used: usize,
    pub(super) stream_id: u32,
    pub(super) active: bool,
    pub(super) complete: bool,
    pub(super) limit: usize,
}

impl HeadersAssembler<'_> {
    pub(super) fn begin(&mut self, stream_id: u32, fragment: &[u8]) -> bool {
        if self.active || fragment.len() > self.limit || fragment.len() > self.buffer.len() {
            self.reset();
            return false;
        }
        self.buffer[..fragment.len()].copy_from_slice(fragment);
        self.used = fragment.len();
        self.stream_id = stream_id;
        self.active = true;
        self.complete = false;
        true
    }

    pub(super) fn append(&mut self, stream_id: u32, fragment: &[u8], end_headers: bool) -> bool {
        if !self.active || self.stream_id != stream_id {
            return false;
        }
        let Some(end) = self.used.checked_add(fragment.len()) else {
            self.reset();
            return false;
        };
        if end > self.limit || end > self.buffer.len() {
            self.reset();
            return false;
        }
        self.buffer[self.used..end].copy_from_slice(fragment);
        self.used = end;
        if end_headers {
            self.active = false;
            self.complete = true;
        }
        true
    }

    pub(super) fn reset(&mut self) {
        self.used = 0;
        self.stream_id = 0;
        self.active = false;
        self.complete = false;
    }
}
