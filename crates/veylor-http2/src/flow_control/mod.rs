#[cfg(test)]
mod tests;

pub const DEFAULT_FLOW_WINDOW_SIZE: i64 = 65_535;
pub const MAX_FLOW_WINDOW_SIZE: i64 = 0x7fff_ffff;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FlowWindow {
    value: i64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FlowWindowError {
    InvalidInitialWindow,
    InvalidAdjustment,
    InvalidIncrement,
    InvalidAmount,
    InsufficientWindow,
    Overflow,
}

impl FlowWindow {
    pub fn new(initial: i64) -> Result<Self, FlowWindowError> {
        if !(0..=MAX_FLOW_WINDOW_SIZE).contains(&initial) {
            return Err(FlowWindowError::InvalidInitialWindow);
        }
        Ok(Self { value: initial })
    }

    pub const fn value(self) -> i64 {
        self.value
    }

    pub fn take(&mut self, count: i64) -> Result<(), FlowWindowError> {
        if count < 0 {
            return Err(FlowWindowError::InvalidAmount);
        }
        if count == 0 {
            return Ok(());
        }
        if count > self.value {
            return Err(FlowWindowError::InsufficientWindow);
        }
        self.value -= count;
        Ok(())
    }

    pub fn adjust(&mut self, delta: i64) -> Result<(), FlowWindowError> {
        if !(-MAX_FLOW_WINDOW_SIZE..=MAX_FLOW_WINDOW_SIZE).contains(&delta) {
            return Err(FlowWindowError::InvalidAdjustment);
        }
        let Some(value) = self.value.checked_add(delta) else {
            return Err(FlowWindowError::Overflow);
        };
        if value > MAX_FLOW_WINDOW_SIZE {
            return Err(FlowWindowError::Overflow);
        }
        self.value = value;
        Ok(())
    }

    pub fn add(&mut self, increment: u32) -> Result<(), FlowWindowError> {
        if increment == 0 || i64::from(increment) > MAX_FLOW_WINDOW_SIZE {
            return Err(FlowWindowError::InvalidIncrement);
        }

        let increment = i64::from(increment);
        let Some(value) = self.value.checked_add(increment) else {
            return Err(FlowWindowError::Overflow);
        };
        if value > MAX_FLOW_WINDOW_SIZE {
            return Err(FlowWindowError::Overflow);
        }
        self.value = value;
        Ok(())
    }
}

pub fn choose_data_frame_size(
    connection_available: i64,
    stream_available: i64,
    remaining: u64,
    max_frame_size: u32,
) -> u32 {
    let available = connection_available.min(stream_available);
    if available <= 0 || remaining == 0 || max_frame_size == 0 {
        return 0;
    }

    let chunk = (available as u64)
        .min(remaining)
        .min(u64::from(max_frame_size.min(crate::frame::MAX_FRAME_SIZE)));
    chunk as u32
}
