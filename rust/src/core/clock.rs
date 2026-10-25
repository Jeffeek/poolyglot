//! Time abstraction so every time-dependent rule is deterministic in tests. Time is measured
//! in `f64` seconds (like every sibling implementation).

use std::sync::Mutex;
use std::time::Instant;

/// A monotonic time source. The pool uses the injected clock for *logical* time (connection
/// age, idle time, breaker recovery, retry backoff); real-time blocking in the async pool is
/// driven by the runtime, not the clock (exactly as Python's `asyncio.timeout` ignores the
/// injected clock).
pub trait Clock: Send + Sync {
    /// Monotonically increasing time, in seconds.
    fn monotonic(&self) -> f64;

    /// Blocks for the given number of seconds (no-op for `seconds <= 0`).
    fn sleep(&self, seconds: f64);

    /// Async sleep for the given number of seconds. For [`SystemClock`] this awaits a real
    /// timer; for [`ManualClock`] it advances logical time and returns immediately.
    #[cfg(feature = "tokio")]
    fn sleep_async<'a>(
        &'a self,
        seconds: f64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>>;
}

/// Real wall-clock time (the default). Anchored at construction so `monotonic` starts near 0.
#[derive(Debug)]
pub struct SystemClock {
    start: Instant,
}

impl SystemClock {
    /// Returns a clock anchored at the current instant.
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
        }
    }
}

impl Default for SystemClock {
    fn default() -> Self {
        Self::new()
    }
}

impl Clock for SystemClock {
    fn monotonic(&self) -> f64 {
        self.start.elapsed().as_secs_f64()
    }

    fn sleep(&self, seconds: f64) {
        if seconds > 0.0 {
            std::thread::sleep(std::time::Duration::from_secs_f64(seconds));
        }
    }

    #[cfg(feature = "tokio")]
    fn sleep_async<'a>(
        &'a self,
        seconds: f64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        Box::pin(async move {
            if seconds > 0.0 {
                tokio::time::sleep(std::time::Duration::from_secs_f64(seconds)).await;
            }
        })
    }
}

/// A fake clock for tests: time moves only when you tell it to; sleeping just advances it.
/// Use `1000.0` as the start to match the other languages' default.
#[derive(Debug)]
pub struct ManualClock {
    now: Mutex<f64>,
}

impl ManualClock {
    /// Returns a manual clock starting at `start` seconds.
    pub fn new(start: f64) -> Self {
        Self {
            now: Mutex::new(start),
        }
    }

    /// Moves the clock forward by `seconds`.
    pub fn advance(&self, seconds: f64) {
        *self.now.lock().unwrap() += seconds;
    }
}

impl Clock for ManualClock {
    fn monotonic(&self) -> f64 {
        *self.now.lock().unwrap()
    }

    fn sleep(&self, seconds: f64) {
        self.advance(seconds);
    }

    #[cfg(feature = "tokio")]
    fn sleep_async<'a>(
        &'a self,
        seconds: f64,
    ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + 'a>> {
        self.advance(seconds);
        // Yield like asyncio.sleep(0): a cancellation point without real waiting.
        Box::pin(async {})
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_advances_and_sleeps() {
        let c = SystemClock::new();
        let t0 = c.monotonic();
        assert!(t0 >= 0.0);
        c.sleep(0.0); // no-op branch
        c.sleep(-1.0); // no-op branch
        c.sleep(0.01);
        assert!(c.monotonic() >= t0);
        // Default + Debug.
        let _ = SystemClock::default();
        assert!(format!("{c:?}").contains("SystemClock"));
    }

    #[test]
    fn manual_clock_is_controllable() {
        let c = ManualClock::new(1000.0);
        assert_eq!(c.monotonic(), 1000.0);
        c.advance(5.0);
        assert_eq!(c.monotonic(), 1005.0);
        c.sleep(2.5);
        assert_eq!(c.monotonic(), 1007.5);
        assert!(format!("{c:?}").contains("ManualClock"));
    }

    #[cfg(feature = "tokio")]
    #[tokio::test]
    async fn sleep_async_system_and_manual() {
        let s = SystemClock::new();
        s.sleep_async(0.0).await; // no-op branch
        s.sleep_async(0.01).await; // real timer branch
        let m = ManualClock::new(0.0);
        m.sleep_async(5.0).await; // advances logical time, returns immediately
        assert_eq!(m.monotonic(), 5.0);
    }
}
