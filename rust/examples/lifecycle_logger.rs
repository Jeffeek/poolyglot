//! A `PoolListener` that logs every lifecycle event (blocking, std only). Run with:
//!
//! ```text
//! cargo run --example lifecycle_logger
//! ```
//!
//! The scenario below triggers each hook: create, acquire/release, max-uses rotation, leak,
//! acquire timeout, create failure and breaker transitions.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread::sleep;
use std::time::{Duration, Instant};

use connpool::{
    BreakerConfig, BreakerState, CloseReason, ConnectionPool, FactoryError, FnFactory, PoolConfig,
    PoolListener, PoolOptions, PooledConnection, RetryPolicy, Timeout,
};

type Conn = Arc<u64>;

/// Prints each event with the time since start.
struct Logger(Instant);

impl Logger {
    fn log(&self, msg: String) {
        println!("[{:>7.3}s] {msg}", self.0.elapsed().as_secs_f64());
    }
}

impl PoolListener<Conn> for Logger {
    fn on_connection_created(&self, c: &PooledConnection<Conn>, duration: f64) {
        self.log(format!("created   #{} in {duration:.4}s", c.id()));
    }
    fn on_connection_closed(&self, c: &PooledConnection<Conn>, reason: CloseReason) {
        self.log(format!("closed    #{} reason={reason} uses={}", c.id(), c.use_count()));
    }
    fn on_create_failed(&self, error: &(dyn Error + 'static)) {
        self.log(format!("create failed: {error}"));
    }
    fn on_acquired(&self, c: &PooledConnection<Conn>, wait: f64) {
        self.log(format!("acquired  #{} waited {wait:.4}s", c.id()));
    }
    fn on_released(&self, c: &PooledConnection<Conn>, held: f64) {
        self.log(format!("released  #{} held {held:.4}s", c.id()));
    }
    fn on_acquire_timeout(&self, waited: f64) {
        self.log(format!("acquire timeout after {waited:.3}s"));
    }
    fn on_connection_leak(&self, c: &PooledConnection<Conn>, held_for: f64) {
        self.log(format!("LEAK?     #{} held {held_for:.3}s", c.id()));
    }
    fn on_breaker_state_change(&self, old: BreakerState, new: BreakerState) {
        self.log(format!("breaker   {} -> {}", old.as_str(), new.as_str()));
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let fail = Arc::new(AtomicBool::new(false));
    let next = Arc::new(AtomicU64::new(0));
    let factory = FnFactory::new({
        let (fail, next) = (fail.clone(), next.clone());
        move || {
            if fail.load(Ordering::SeqCst) {
                return Err::<Conn, FactoryError>("backend down".into());
            }
            Ok(Arc::new(next.fetch_add(1, Ordering::SeqCst)))
        }
    });

    let config = PoolConfig {
        max_size: 1,
        max_uses: Some(2),
        leak_detection_timeout: Some(0.02),
        maintenance_interval: None, // sweep manually below
        retry: RetryPolicy::no_retry(),
        breaker: Some(BreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        }),
        ..Default::default()
    };
    let pool = ConnectionPool::new(
        factory,
        config,
        Some(PoolOptions {
            name: Some("logged".into()),
            listeners: vec![Arc::new(Logger(Instant::now()))],
            ..Default::default()
        }),
    )?;

    println!("-- reuse, then rotation at max_uses=2");
    for _ in 0..3 {
        let c = pool.acquire(Timeout::Unset)?;
        pool.release(c)?;
    }

    println!("-- leak detection (held longer than 20ms, flagged by maintenance)");
    let held = pool.acquire(Timeout::Unset)?;
    sleep(Duration::from_millis(40));
    pool.run_maintenance();

    println!("-- acquire timeout (pool of 1 is exhausted)");
    let _ = pool.acquire(Timeout::Of(0.05));
    pool.release(held)?;

    println!("-- backend down: create failures trip the breaker");
    let c = pool.acquire(Timeout::Unset)?;
    pool.discard(c)?;
    fail.store(true, Ordering::SeqCst);
    for _ in 0..3 {
        let _ = pool.acquire(Timeout::Of(0.5));
    }

    pool.close(None);
    Ok(())
}
