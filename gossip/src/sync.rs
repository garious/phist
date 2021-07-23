use std::{
    collections::VecDeque,
    sync::{
        Arc, Condvar, LockResult, Mutex, PoisonError, RwLockReadGuard, RwLockWriteGuard,
        TryLockError, TryLockResult,
    },
};

/// FIFO reader-writer lock.
#[derive(Default)]
pub struct RwLock<T> {
    inner: std::sync::RwLock<T>,
    queue: Mutex<VecDeque<Arc<Condvar>>>,
}

impl<T> RwLock<T> {
    pub fn new(t: T) -> Self {
        Self {
            inner: std::sync::RwLock::new(t),
            queue: Mutex::default(),
        }
    }

    pub fn read(&self) -> LockResult<RwLockReadGuard<'_, T>> {
        use std::sync::RwLock;
        self.lock(RwLock::read, RwLock::try_read)
    }

    pub fn write(&self) -> LockResult<RwLockWriteGuard<'_, T>> {
        use std::sync::RwLock;
        self.lock(RwLock::write, RwLock::try_write)
    }

    fn lock<'a, F1, F2, R: 'a>(&'a self, lock: F1, try_lock: F2) -> LockResult<R>
    where
        F1: Fn(&'a std::sync::RwLock<T>) -> LockResult<R>,
        F2: Fn(&'a std::sync::RwLock<T>) -> TryLockResult<R>,
    {
        let mut queue = match self.queue.lock() {
            Ok(queue) => queue,
            Err(_) => {
                let guard = lock(&self.inner);
                return guard.and_then(|r| Err(PoisonError::new(r)));
            }
        };
        // If the queue is empty and no one is waiting, then try lock the inner
        // and return if it does not block.
        if queue.is_empty() {
            match try_lock(&self.inner) {
                Ok(inner) => return Ok(inner),
                Err(TryLockError::Poisoned(err)) => return Err(err),
                Err(TryLockError::WouldBlock) => (),
            }
        };
        // Add self at the end of the queue and
        // wait until reach head of the queue.
        let cvar: Arc<Condvar> = Arc::default();
        queue.push_back(Arc::clone(&cvar));
        while !Arc::ptr_eq(&cvar, queue.front().unwrap()) {
            queue = match cvar.wait(queue) {
                Ok(queue) => queue,
                Err(_) => {
                    let guard = lock(&self.inner);
                    return guard.and_then(|r| Err(PoisonError::new(r)));
                }
            };
            // Check if it is in fact in front of the queue;
            // Otherwise this has been a spurious wakeup.
        }
        // Should drop queue lock here, so that other threads are not blocked
        // and are queued in order of their arrival.
        drop(queue);
        let guard = lock(&self.inner);
        // Pop self from the queue, and notify the next one in line that it is
        // now head of the queue.
        let cvar: Arc<Condvar> = match self.queue.lock() {
            Err(_) => {
                return guard.and_then(|r| Err(PoisonError::new(r)));
            }
            Ok(mut queue) => {
                debug_assert!(Arc::ptr_eq(&cvar, queue.front().unwrap()));
                queue.pop_front();
                match queue.front() {
                    Some(cvar) => Arc::clone(cvar),
                    None => return guard, // Queue is empty; no one to notify.
                }
            }
        };
        cvar.notify_one();
        guard
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        rand::Rng,
        std::{sync::Barrier, thread},
    };

    #[test]
    fn test_fifo_rwlock() {
        const NUM_READERS: usize = 10;
        const NUM_WRITERS: usize = 10;
        const NUM_REPS: usize = 10000;
        let mut writer_handles = Vec::with_capacity(NUM_WRITERS);
        let mut reader_handles = Vec::with_capacity(NUM_READERS);
        let barrier = Arc::new(Barrier::new(NUM_READERS + NUM_WRITERS));
        let var: Arc<RwLock<i64>> = Arc::default();
        for _ in 0..NUM_WRITERS {
            let barrier = Arc::clone(&barrier);
            let var = Arc::clone(&var);
            writer_handles.push(thread::spawn(move || {
                let mut rng = rand::thread_rng();
                barrier.wait();
                let mut out = 0;
                for _ in 0..NUM_REPS {
                    let mut var = var.write().unwrap();
                    let sample = rng.gen_range(1, 100);
                    *var += sample;
                    out += sample;
                }
                out
            }));
        }
        for _ in 0..NUM_READERS {
            let barrier = Arc::clone(&barrier);
            let var = Arc::clone(&var);
            reader_handles.push(thread::spawn(move || {
                barrier.wait();
                let mut out = 0;
                for _ in 0..NUM_REPS {
                    let var = var.read().unwrap();
                    out = *var;
                }
                out
            }))
        }
        let mut acc = 0;
        for handle in writer_handles {
            acc += handle.join().unwrap();
        }
        assert_eq!(acc, *var.read().unwrap());
        for handle in reader_handles {
            let val = handle.join().unwrap();
            assert!(val <= acc);
            assert!(val * 5 > acc);
        }
    }
}
