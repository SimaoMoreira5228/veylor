use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};

use crate::store::{Balance, Store, StoreError, Write};

pub const QUEUE_CAP: usize = 1024;
pub const BATCH_CAP: usize = 256;

// NOTE: only the writer thread waits on SQLite locks, so a second process sharing the
// database file queues behind this wait instead of failing the request at once.
pub const BUSY_WAIT: Duration = Duration::from_millis(400);

// NOTE: a 503 from this module means "not acknowledged", never "not applied". A batch can
// commit after its waiter gave up (deadline, disconnect, dropped response). Clients must
// retry with the same Idempotency-Key; the replay returns the original balance.
pub const COMMIT_WAIT: Duration = Duration::from_millis(1500);

struct Slot {
    state: Mutex<SlotState>,
}

struct SlotState {
    result: Option<Result<Balance, StoreError>>,
    waker: Option<Waker>,
}

impl Slot {
    fn complete(&self, result: Result<Balance, StoreError>) {
        let waker = {
            let mut state = self.state.lock();
            state.result = Some(result);
            state.waker.take()
        };

        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct Job {
    write: Write,
    slot: Arc<Slot>,
}

struct Pending {
    jobs: VecDeque<Job>,
    closed: bool,
}

struct Queue {
    pending: Mutex<Pending>,
    ready: Condvar,
}

pub struct Writer {
    queue: Arc<Queue>,
    thread: Mutex<Option<JoinHandle<()>>>,
}

pub struct Ticket {
    slot: Arc<Slot>,
    deadline: Instant,
}

impl Writer {
    pub fn start(store: Store, commit_delay: Duration) -> Self {
        let queue = Arc::new(Queue {
            pending: Mutex::new(Pending {
                jobs: VecDeque::with_capacity(QUEUE_CAP),
                closed: false,
            }),
            ready: Condvar::new(),
        });
        let worker = Arc::clone(&queue);
        let thread = std::thread::Builder::new()
            .name("ledger-writer".to_owned())
            .spawn(move || run(store, &worker, commit_delay))
            .expect("spawn ledger writer");

        Self {
            queue,
            thread: Mutex::new(Some(thread)),
        }
    }

    pub fn submit(&self, write: Write) -> Result<Ticket, StoreError> {
        let slot = Arc::new(Slot {
            state: Mutex::new(SlotState {
                result: None,
                waker: None,
            }),
        });

        {
            let mut pending = self.queue.pending.lock();
            if pending.closed || pending.jobs.len() >= QUEUE_CAP {
                return Err(StoreError::Busy);
            }

            pending.jobs.push_back(Job {
                write,
                slot: Arc::clone(&slot),
            });
        }
        self.queue.ready.notify_one();

        Ok(Ticket {
            slot,
            deadline: Instant::now() + COMMIT_WAIT,
        })
    }

    pub fn close(&self) {
        self.queue.pending.lock().closed = true;
        self.queue.ready.notify_all();

        if let Some(thread) = self.thread.lock().take() {
            let _ = thread.join();
        }
    }
}

impl Drop for Writer {
    fn drop(&mut self) {
        self.close();
    }
}

impl Future for Ticket {
    type Output = Result<Balance, StoreError>;

    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.slot.state.lock();
        if let Some(result) = state.result.take() {
            return Poll::Ready(result);
        }

        if Instant::now() >= self.deadline {
            return Poll::Ready(Err(StoreError::Timeout));
        }

        state.waker = Some(context.waker().clone());

        Poll::Pending
    }
}

fn run(mut store: Store, queue: &Queue, commit_delay: Duration) {
    let mut batch: Vec<Job> = Vec::with_capacity(BATCH_CAP);

    loop {
        {
            let mut pending = queue.pending.lock();
            while pending.jobs.is_empty() {
                if pending.closed {
                    return;
                }

                queue.ready.wait(&mut pending);
            }

            let take = pending.jobs.len().min(BATCH_CAP);
            batch.extend(pending.jobs.drain(..take));
        }

        let writes: Vec<&Write> = batch.iter().map(|job| &job.write).collect();
        let results = store.commit_batch(&writes, commit_delay);

        for (job, result) in batch.drain(..).zip(results) {
            job.slot.complete(result);
        }
    }
}
