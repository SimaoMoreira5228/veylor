pub const MAX_TIMER_CAPACITY: usize = 0x7fff_ffff;
pub const TIMER_INDEX_STORAGE_FACTOR: usize = 3;
const TIMER_INDEX_EMPTY: u32 = u32::MAX;
const TIMER_INDEX_DELETED: u32 = u32::MAX - 1;
const NANOS_PER_MILLI: u64 = 1_000_000;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TimerExpiration {
    pub timer_id: u64,
    pub deadline_ns: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimerQueueError {
    Invalid,
    Full,
    Duplicate,
    NotFound,
}

pub struct TimerQueue<'storage, 'index> {
    storage: &'storage mut [TimerExpiration],
    index_storage: &'index mut [u32],
    count: usize,
    deleted_index_count: usize,
}

impl<'storage, 'index> TimerQueue<'storage, 'index> {
    pub fn new(
        storage: &'storage mut [TimerExpiration],
        index_storage: &'index mut [u32],
    ) -> Result<Self, TimerQueueError> {
        let capacity = storage.len();
        if capacity > MAX_TIMER_CAPACITY {
            return Err(TimerQueueError::Invalid);
        }
        let index_length = capacity
            .checked_mul(TIMER_INDEX_STORAGE_FACTOR)
            .ok_or(TimerQueueError::Invalid)?;
        if index_storage.len() < index_length {
            return Err(TimerQueueError::Invalid);
        }
        index_storage[..index_length].fill(TIMER_INDEX_EMPTY);

        Ok(Self {
            storage,
            index_storage: &mut index_storage[..index_length],
            count: 0,
            deleted_index_count: 0,
        })
    }

    pub fn len(&self) -> usize {
        self.count
    }

    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    pub fn capacity(&self) -> usize {
        self.storage.len()
    }

    pub fn schedule(&mut self, timer: TimerExpiration) -> Result<(), TimerQueueError> {
        self.prepare_index()?;
        let (existing, bucket) = self.find_index(timer.timer_id)?;
        if existing.is_some() {
            return Err(TimerQueueError::Duplicate);
        }
        if self.count == self.storage.len() {
            return Err(TimerQueueError::Full);
        }
        let bucket = bucket.ok_or(TimerQueueError::Invalid)?;
        let heap_index = u32::try_from(self.count).map_err(|_| TimerQueueError::Invalid)?;
        let bucket_index = u32::try_from(bucket).map_err(|_| TimerQueueError::Invalid)?;

        let index = self.count;
        self.storage[index] = timer;
        self.count += 1;
        self.insert_index(bucket, heap_index, bucket_index);
        self.sift_up(index);
        Ok(())
    }

    pub fn cancel(&mut self, timer_id: u64) -> Result<(), TimerQueueError> {
        let (index, _) = self.find_index(timer_id)?;
        let index = index.ok_or(TimerQueueError::NotFound)?;
        self.remove_at(index)
    }

    pub fn pop_expired(&mut self, now_ns: u64) -> Result<Option<TimerExpiration>, TimerQueueError> {
        if self.count == 0 || self.storage[0].deadline_ns > now_ns {
            return Ok(None);
        }
        let timer = self.storage[0];
        self.remove_at(0)?;
        Ok(Some(timer))
    }

    pub fn wait_timeout_ms(&self, now_ns: u64, max_wait_ms: i32) -> i32 {
        if max_wait_ms == 0 {
            return 0;
        }
        if self.count == 0 {
            return max_wait_ms;
        }

        let deadline_ns = self.storage[0].deadline_ns;
        if deadline_ns <= now_ns {
            return 0;
        }

        let remaining_ns = deadline_ns - now_ns;
        let wait_ms = remaining_ns / NANOS_PER_MILLI
            + u64::from(!remaining_ns.is_multiple_of(NANOS_PER_MILLI));
        if max_wait_ms >= 0 && wait_ms > max_wait_ms as u64 {
            return max_wait_ms;
        }
        wait_ms.min(i32::MAX as u64) as i32
    }

    fn prepare_index(&mut self) -> Result<(), TimerQueueError> {
        let hash_capacity = self.storage.len() * 2;
        let deleted_limit = (hash_capacity / 4).max(1);
        if self.deleted_index_count < deleted_limit {
            return Ok(());
        }
        self.rebuild_index()
    }

    fn find_index(&self, timer_id: u64) -> Result<(Option<usize>, Option<usize>), TimerQueueError> {
        let hash_capacity = self.storage.len() * 2;
        if hash_capacity == 0 {
            return Ok((None, None));
        }

        let hash = &self.index_storage[..hash_capacity];
        let start = (timer_id % hash_capacity as u64) as usize;
        let mut first_deleted = None;
        for probe in 0..hash_capacity {
            let bucket = probe_bucket(start, probe, hash_capacity);
            let heap_index = hash[bucket];
            if heap_index == TIMER_INDEX_EMPTY {
                return Ok((None, Some(first_deleted.unwrap_or(bucket))));
            }
            if heap_index == TIMER_INDEX_DELETED {
                if first_deleted.is_none() {
                    first_deleted = Some(bucket);
                }
                continue;
            }

            let heap_index = usize::try_from(heap_index).map_err(|_| TimerQueueError::Invalid)?;
            let timer = self
                .storage
                .get(heap_index)
                .filter(|_| heap_index < self.count)
                .ok_or(TimerQueueError::Invalid)?;
            if timer.timer_id == timer_id {
                return Ok((Some(heap_index), Some(bucket)));
            }
        }
        Ok((None, first_deleted))
    }

    fn insert_index(&mut self, bucket: usize, heap_index: u32, bucket_index: u32) {
        let hash_capacity = self.storage.len() * 2;
        let hash = &mut self.index_storage[..hash_capacity];
        if hash[bucket] == TIMER_INDEX_DELETED {
            self.deleted_index_count -= 1;
        }
        hash[bucket] = heap_index;
        self.index_storage[hash_capacity + heap_index as usize] = bucket_index;
    }

    fn rebuild_index(&mut self) -> Result<(), TimerQueueError> {
        let hash_capacity = self.storage.len() * 2;
        let (hash_storage, inverse_storage) = self.index_storage.split_at_mut(hash_capacity);
        hash_storage.fill(TIMER_INDEX_EMPTY);
        inverse_storage.fill(TIMER_INDEX_EMPTY);
        self.deleted_index_count = 0;

        for heap_index in 0..self.count {
            let timer_id = self.storage[heap_index].timer_id;
            let start = (timer_id % hash_capacity as u64) as usize;
            let bucket = (0..hash_capacity)
                .map(|probe| probe_bucket(start, probe, hash_capacity))
                .find(|candidate| hash_storage[*candidate] == TIMER_INDEX_EMPTY)
                .ok_or(TimerQueueError::Invalid)?;
            let heap_index = u32::try_from(heap_index).map_err(|_| TimerQueueError::Invalid)?;
            let bucket_index = u32::try_from(bucket).map_err(|_| TimerQueueError::Invalid)?;
            hash_storage[bucket] = heap_index;
            inverse_storage[heap_index as usize] = bucket_index;
        }
        Ok(())
    }

    fn sift_up(&mut self, start: usize) {
        let mut index = start;
        while index > 0 {
            let parent = (index - 1) / 2;
            if !timer_precedes(self.storage[index], self.storage[parent]) {
                return;
            }
            self.swap_entries(index, parent);
            index = parent;
        }
    }

    fn sift_down(&mut self, start: usize) {
        let mut index = start;
        loop {
            let left = index * 2 + 1;
            if left >= self.count {
                return;
            }
            let right = left + 1;
            let first =
                if right < self.count && timer_precedes(self.storage[right], self.storage[left]) {
                    right
                } else {
                    left
                };
            if !timer_precedes(self.storage[first], self.storage[index]) {
                return;
            }
            self.swap_entries(index, first);
            index = first;
        }
    }

    fn swap_entries(&mut self, left: usize, right: usize) {
        let hash_capacity = self.storage.len() * 2;
        let inverse_start = hash_capacity;
        let left_bucket = self.index_storage[inverse_start + left] as usize;
        let right_bucket = self.index_storage[inverse_start + right] as usize;
        self.storage.swap(left, right);
        self.index_storage[left_bucket] = right as u32;
        self.index_storage[right_bucket] = left as u32;
        self.index_storage[inverse_start + left] = right_bucket as u32;
        self.index_storage[inverse_start + right] = left_bucket as u32;
    }

    fn remove_at(&mut self, index: usize) -> Result<(), TimerQueueError> {
        if index >= self.count {
            return Err(TimerQueueError::Invalid);
        }
        let last = self.count - 1;
        let hash_capacity = self.storage.len() * 2;
        let inverse_start = hash_capacity;
        let bucket = self.index_storage[inverse_start + index] as usize;
        if bucket >= hash_capacity || self.index_storage[bucket] as usize != index {
            return Err(TimerQueueError::Invalid);
        }
        let last_bucket = if index != last {
            let last_bucket = self.index_storage[inverse_start + last] as usize;
            if last_bucket >= hash_capacity || self.index_storage[last_bucket] as usize != last {
                return Err(TimerQueueError::Invalid);
            }
            Some(last_bucket)
        } else {
            None
        };
        self.index_storage[bucket] = TIMER_INDEX_DELETED;
        self.index_storage[inverse_start + index] = TIMER_INDEX_EMPTY;
        self.deleted_index_count += 1;
        self.count = last;

        if index == last {
            self.storage[last] = TimerExpiration::default();
            return Ok(());
        }

        let last_bucket = last_bucket.ok_or(TimerQueueError::Invalid)?;
        self.storage[index] = self.storage[last];
        self.storage[last] = TimerExpiration::default();
        self.index_storage[last_bucket] =
            u32::try_from(index).map_err(|_| TimerQueueError::Invalid)?;
        self.index_storage[inverse_start + index] =
            u32::try_from(last_bucket).map_err(|_| TimerQueueError::Invalid)?;
        self.index_storage[inverse_start + last] = TIMER_INDEX_EMPTY;

        if index > 0 && timer_precedes(self.storage[index], self.storage[(index - 1) / 2]) {
            self.sift_up(index);
        } else {
            self.sift_down(index);
        }
        Ok(())
    }
}

fn timer_precedes(left: TimerExpiration, right: TimerExpiration) -> bool {
    left.deadline_ns < right.deadline_ns
        || (left.deadline_ns == right.deadline_ns && left.timer_id < right.timer_id)
}

pub(super) fn probe_bucket(start: usize, probe: usize, capacity: usize) -> usize {
    let until_wrap = capacity - start;
    if probe < until_wrap {
        start + probe
    } else {
        probe - until_wrap
    }
}
