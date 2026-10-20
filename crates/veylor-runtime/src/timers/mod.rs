mod queue;

pub use queue::{
    MAX_TIMER_CAPACITY, TIMER_INDEX_STORAGE_FACTOR, TimerExpiration, TimerQueue, TimerQueueError,
};

#[cfg(test)]
mod tests;
