use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};
use std::time::Duration;

use parking_lot::{Condvar, Mutex};
use veylor_web::Executor;

pub const THREADS: usize = 128;
pub const QUEUE_CAP: usize = 256;

const RECHECK: Duration = Duration::from_millis(5);

type Task = Pin<Box<dyn Future<Output = ()> + Send + 'static>>;

struct Shared {
    tasks: Mutex<VecDeque<Task>>,
    ready: Condvar,
}

// NOTE: the engine's default executor drives each handler on the dispatcher thread, which
// would serialize commit waiters and defeat group commit. This fixed pool lets many
// handlers wait on the writer at once. When the queue is full the task runs on the
// dispatcher, so overload slows intake instead of growing memory or spawning threads.
pub struct HandlerPool {
    shared: Arc<Shared>,
}

impl HandlerPool {
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            tasks: Mutex::new(VecDeque::with_capacity(QUEUE_CAP)),
            ready: Condvar::new(),
        });

        for index in 0..THREADS {
            let worker = Arc::clone(&shared);
            std::thread::Builder::new()
                .name(format!("ledger-handler-{index}"))
                .spawn(move || work(&worker))
                .expect("spawn ledger handler thread");
        }

        Self { shared }
    }
}

impl Executor for HandlerPool {
    fn spawn(&self, task: Task) {
        let rejected = {
            let mut tasks = self.shared.tasks.lock();
            if tasks.len() >= QUEUE_CAP {
                Some(task)
            } else {
                tasks.push_back(task);
                None
            }
        };

        match rejected {
            Some(task) => block_on(task),
            None => {
                self.shared.ready.notify_one();
            }
        }
    }
}

fn work(shared: &Shared) {
    loop {
        let task = {
            let mut tasks = shared.tasks.lock();
            loop {
                if let Some(task) = tasks.pop_front() {
                    break task;
                }

                shared.ready.wait(&mut tasks);
            }
        };

        block_on(task);
    }
}

struct Unpark(std::thread::Thread);

impl Wake for Unpark {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on(mut task: Task) {
    let waker = Waker::from(Arc::new(Unpark(std::thread::current())));
    let mut context = Context::from_waker(&waker);

    while task.as_mut().poll(&mut context).is_pending() {
        std::thread::park_timeout(RECHECK);
    }
}
