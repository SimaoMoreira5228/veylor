#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct Utf8 {
    remaining: u8,
    code_point: u32,
    minimum: u32,
}

impl Utf8 {
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    pub(crate) fn finish(self) -> bool {
        self.remaining == 0
    }

    pub(crate) fn feed(&mut self, bytes: &[u8]) -> bool {
        for &byte in bytes {
            if self.remaining == 0 {
                match byte {
                    0x00..=0x7f => {}
                    0xc2..=0xdf => {
                        self.remaining = 1;
                        self.code_point = u32::from(byte & 0x1f);
                        self.minimum = 0x80;
                    }
                    0xe0..=0xef => {
                        self.remaining = 2;
                        self.code_point = u32::from(byte & 0x0f);
                        self.minimum = 0x800;
                    }
                    0xf0..=0xf4 => {
                        self.remaining = 3;
                        self.code_point = u32::from(byte & 0x07);
                        self.minimum = 0x1_0000;
                    }
                    _ => return false,
                }
                continue;
            }

            if byte & 0xc0 != 0x80 {
                return false;
            }
            self.code_point = (self.code_point << 6) | u32::from(byte & 0x3f);
            self.remaining -= 1;
            if self.remaining != 0 {
                continue;
            }
            if self.code_point < self.minimum
                || self.code_point > 0x10_ffff
                || (0xd800..=0xdfff).contains(&self.code_point)
            {
                return false;
            }
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::Utf8;

    #[test]
    fn accepts_scalar_values_split_at_every_boundary() {
        for text in ["a", "é", "€", "😀", "aé€😀"] {
            let bytes = text.as_bytes();
            for split in 0..=bytes.len() {
                let mut utf8 = Utf8::default();
                assert!(utf8.feed(&bytes[..split]));
                assert!(utf8.feed(&bytes[split..]));
                assert!(utf8.finish());
            }
        }
    }

    #[test]
    fn rejects_truncation_overlong_surrogate_and_out_of_range() {
        for bytes in [
            &[0xe2, 0x82][..],
            &[0xc0, 0x80][..],
            &[0xe0, 0x80, 0x80][..],
            &[0xed, 0xa0, 0x80][..],
            &[0xf4, 0x90, 0x80, 0x80][..],
            &[0x80][..],
        ] {
            let mut utf8 = Utf8::default();
            assert!(!utf8.feed(bytes) || !utf8.finish(), "{bytes:?}");
        }
    }
}
