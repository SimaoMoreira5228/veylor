use super::queue::probe_bucket;
use super::{TIMER_INDEX_STORAGE_FACTOR, TimerExpiration, TimerQueue, TimerQueueError};

fn expiration(timer_id: u64, deadline_ns: u64) -> TimerExpiration {
    TimerExpiration {
        timer_id,
        deadline_ns,
    }
}

#[test]
fn pops_expired_timers_by_deadline_then_id() {
    let mut storage = [TimerExpiration::default(); 4];
    let mut index_storage = [0; 4 * TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut storage, &mut index_storage).unwrap();
    timers.schedule(expiration(4, 20)).unwrap();
    timers.schedule(expiration(3, 10)).unwrap();
    timers.schedule(expiration(2, 10)).unwrap();

    assert_eq!(timers.pop_expired(9), Ok(None));
    assert_eq!(timers.len(), 3);
    assert_eq!(timers.pop_expired(10), Ok(Some(expiration(2, 10))));
    assert_eq!(timers.pop_expired(10), Ok(Some(expiration(3, 10))));
    assert_eq!(timers.pop_expired(19), Ok(None));
    assert_eq!(timers.len(), 1);
}

#[test]
fn rejects_duplicate_ids_and_capacity_overflow() {
    let mut storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut storage, &mut index_storage).unwrap();
    timers.schedule(expiration(1, 10)).unwrap();
    assert_eq!(
        timers.schedule(expiration(1, 20)),
        Err(TimerQueueError::Duplicate)
    );
    assert_eq!(
        timers.schedule(expiration(2, 20)),
        Err(TimerQueueError::Full)
    );
    assert_eq!(timers.len(), 1);
}

#[test]
fn cancels_root_and_interior_entries_then_reuses_slots() {
    let mut storage = [TimerExpiration::default(); 5];
    let mut index_storage = [0; 5 * TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut storage, &mut index_storage).unwrap();
    timers.schedule(expiration(4, 40)).unwrap();
    timers.schedule(expiration(1, 10)).unwrap();
    timers.schedule(expiration(3, 30)).unwrap();
    timers.schedule(expiration(2, 20)).unwrap();

    assert_eq!(timers.cancel(1), Ok(()));
    assert_eq!(timers.cancel(3), Ok(()));
    assert_eq!(timers.cancel(3), Err(TimerQueueError::NotFound));
    assert_eq!(timers.len(), 2);
    assert_eq!(timers.pop_expired(40), Ok(Some(expiration(2, 20))));
    assert_eq!(timers.pop_expired(40), Ok(Some(expiration(4, 40))));
    assert!(timers.is_empty());

    timers.schedule(expiration(2, 5)).unwrap();
    assert_eq!(timers.cancel(2), Ok(()));
    assert_eq!(timers.cancel(2), Err(TimerQueueError::NotFound));
}

#[test]
fn rounds_wait_up_and_respects_maximum_and_epoll_limits() {
    let mut storage = [TimerExpiration::default(); 2];
    let mut index_storage = [0; 2 * TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut storage, &mut index_storage).unwrap();
    assert_eq!(timers.wait_timeout_ms(100, 50), 50);
    timers.schedule(expiration(1, 1_000_002)).unwrap();
    assert_eq!(timers.wait_timeout_ms(1, 50), 2);
    assert_eq!(timers.wait_timeout_ms(1, 1), 1);
    assert_eq!(timers.wait_timeout_ms(1, -1), 2);
    assert_eq!(timers.wait_timeout_ms(1_000_002, -1), 0);

    let mut storage = [TimerExpiration::default(); 1];
    let mut index_storage = [0; TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut storage, &mut index_storage).unwrap();
    timers.schedule(expiration(1, u64::MAX)).unwrap();
    assert_eq!(timers.wait_timeout_ms(0, -1), i32::MAX);
}

#[test]
fn validates_caller_index_storage() {
    let mut storage = [TimerExpiration::default(); 2];
    let mut short_index_storage = [0; 5];
    assert!(matches!(
        TimerQueue::new(&mut storage, &mut short_index_storage),
        Err(TimerQueueError::Invalid)
    ));

    let mut empty_storage = [];
    let mut empty_index_storage = [];
    let mut timers = TimerQueue::new(&mut empty_storage, &mut empty_index_storage).unwrap();
    assert_eq!(
        timers.schedule(expiration(1, 1)),
        Err(TimerQueueError::Full)
    );
}

#[test]
fn hash_probes_wrap_without_usize_overflow() {
    let capacity = u32::MAX as usize - 1;
    assert_eq!(probe_bucket(capacity - 2, 0, capacity), capacity - 2);
    assert_eq!(probe_bucket(capacity - 2, 1, capacity), capacity - 1);
    assert_eq!(probe_bucket(capacity - 2, 2, capacity), 0);
    assert_eq!(probe_bucket(capacity - 2, 5, capacity), 3);
}

#[test]
fn index_survives_heap_swaps_tombstones_and_rebuilds() {
    let mut storage = [TimerExpiration::default(); 8];
    let mut index_storage = [0; 8 * TIMER_INDEX_STORAGE_FACTOR];
    let mut timers = TimerQueue::new(&mut storage, &mut index_storage).unwrap();
    let timer_ids = [1, 2, 3, 4, 5, 6, 7, 8];
    let deadlines = [80, 20, 60, 40, 10, 70, 30, 50];
    for index in 0..timer_ids.len() {
        timers
            .schedule(expiration(timer_ids[index], deadlines[index]))
            .unwrap();
    }

    for timer_id in [2, 4, 6, 8] {
        assert_eq!(timers.cancel(timer_id), Ok(()));
    }
    for (timer_id, deadline_ns) in [(17, 35), (25, 25), (33, 15), (41, 5)] {
        timers.schedule(expiration(timer_id, deadline_ns)).unwrap();
    }
    for timer_id in [1, 3, 5, 7, 17, 25, 33, 41] {
        assert_eq!(timers.cancel(timer_id), Ok(()));
    }
    assert!(timers.is_empty());

    timers.schedule(expiration(2, 5)).unwrap();
    assert_eq!(timers.cancel(2), Ok(()));
    assert_eq!(timers.cancel(2), Err(TimerQueueError::NotFound));
}
