use std::{
    sync::{Condvar, Mutex},
    time::Duration,
};

// encapsulate complications of unneeded mutex and Condvar to give us event behavior of wait and notify
// this will likely be wrapped in an arc somehow
#[derive(Default, Debug)]
pub struct WaitableCondvar {
    pub mutex: Mutex<u8>,
    pub event: Condvar,
}

impl WaitableCondvar {
    pub fn notify_all(&self) {
        self.event.notify_all();
    }
    pub fn wait_timeout(&self, timeout: Duration) -> bool {
        let lock = self.mutex.lock().unwrap();
        let res = self.event.wait_timeout(lock, timeout).unwrap();
        if res.1.timed_out() {
            return true;
        }
        false
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;
    use std::{
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
        thread::Builder,
    };
    #[ignore]
    #[test]
    fn test_waitable_condvar() {
        let data = Arc::new(AtomicBool::new(false));
        let data_ = data.clone();
        let cv = Arc::new(WaitableCondvar::default());
        let cv2 = Arc::new(WaitableCondvar::default());
        let cv_ = cv.clone();
        let cv2_ = cv2.clone();
        let cv2__ = cv2.clone();
        let waiters_active = Arc::new(AtomicUsize::new(0));
        let waiters_active_ = waiters_active.clone();
        let waiters_active__ = waiters_active.clone();
        // several passes to check re-notification and drop one of the
        let passes = 3;
        let handle = Builder::new().spawn(move || {
            for _pass in 0..passes {
                let mut notified = false;
                let mut timed_out = false;
                waiters_active_.fetch_add(1, Ordering::SeqCst);
                while cv2_.wait_timeout(Duration::from_millis(1)) {
                    timed_out = true;
                    if !notified && data_.load(Ordering::SeqCst) {
                        notified = true;
                        cv_.notify_all();
                    }
                }
                assert!(timed_out); // we should have timed out at least once
                assert!(data_.swap(false, Ordering::SeqCst));
            }
        });
        // just wait, but 1 less pass - verifies that notify_all works with multiple and with 1
        let handle2 = Builder::new().spawn(move || {
            for _pass in 0..(passes - 1) {
                waiters_active__.fetch_add(1, Ordering::SeqCst);
                assert!(!cv2__.wait_timeout(Duration::from_millis(10000))); // long enough to not be intermittent, short enough to fail if we really don't get notified
            }
            drop(cv2__);
        });
        let mut expected_waiters = 0;
        for pass in 0..passes {
            expected_waiters += if pass == passes -1 {1} else {2};
            // make sure all expected waiters are waiting
            while waiters_active.load(Ordering::SeqCst) != expected_waiters {
                std::thread::sleep(Duration::from_millis(1));
            }
            assert!(cv.wait_timeout(Duration::from_millis(1)));
            assert!(!data.swap(true, Ordering::SeqCst));
            assert!(!cv.wait_timeout(Duration::from_millis(10000))); // should barely wait, but don't want intermittent
            cv2.notify_all();
        }
        assert!(handle.unwrap().join().is_ok());
        assert!(handle2.unwrap().join().is_ok());
    }
}
