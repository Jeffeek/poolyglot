//! Fuzz target for the divergent async primitives: `tokio::sync::Notify` waiter coordination
//! (`src/pools/async_pool.rs`) and the Drop-guard cancellation path (`SlotGuard`/`BorrowGuard`)
//! that stands in for a cancellation token in Rust's "cancel = drop the future" model. Driven
//! through the public `AsyncConnectionPool<T>` surface, including dropping an in-flight
//! `acquire`/`release` future mid-poll via `tokio::time::timeout` on a tiny pool so waiters queue
//! and the create/borrow cancellation windows get exercised with adversarial byte-driven timing
//! (the same technique the library's own tests use, e.g. async_pool.rs:1091).
#![no_main]

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use connpool::{AsyncConnectionFactory, AsyncConnectionPool, FactoryError, PoolConfig, Timeout};
use libfuzzer_sys::fuzz_target;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct Conn(u64);

struct Factory {
    next: AtomicU64,
}

impl AsyncConnectionFactory<Conn> for Factory {
    async fn create(&self) -> Result<Conn, FactoryError> {
        tokio::time::sleep(Duration::from_micros(50)).await;
        Ok(Conn(self.next.fetch_add(1, Ordering::SeqCst)))
    }
    async fn close(&self, _conn: &Conn) -> Result<(), FactoryError> {
        tokio::time::sleep(Duration::from_micros(10)).await;
        Ok(())
    }
}

fuzz_target!(|data: &[u8]| {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    rt.block_on(run(data));
});

async fn run(data: &[u8]) {
    let cfg = PoolConfig {
        max_size: 2,
        min_size: 0,
        maintenance_interval: None,
        acquire_timeout: Some(0.2),
        ..Default::default()
    };
    let factory = Factory {
        next: AtomicU64::new(0),
    };
    let pool = Arc::new(AsyncConnectionPool::new(factory, cfg, None).expect("valid config"));
    pool.open().await.expect("open");

    let mut held: Vec<Conn> = Vec::new();

    // Bound the op count so one input can't run forever under libFuzzer's watchdog.
    for &op in data.iter().take(32) {
        match op % 4 {
            0 => {
                // Plain acquire, run to completion.
                if let Ok(c) = pool.acquire(Timeout::Unset).await {
                    held.push(c);
                }
            }
            1 => {
                // Release the oldest held connection, if any.
                if !held.is_empty() {
                    let c = held.remove(0);
                    let _ = pool.release(c).await;
                }
            }
            2 => {
                // Acquire, then drop the future mid-flight (the fuzz byte picks the delay) —
                // exercises SlotGuard/BorrowGuard unwinding a reserved slot / briefly-borrowed
                // connection on cancellation.
                let delay_us = u64::from(op) % 100;
                let fut = pool.acquire(Timeout::Unset);
                tokio::pin!(fut);
                if let Ok(res) =
                    tokio::time::timeout(Duration::from_micros(delay_us), &mut fut).await
                {
                    if let Ok(c) = res {
                        held.push(c);
                    }
                }
                // else: future dropped here, guard runs synchronously in Drop.
            }
            _ => {
                // Release, but drop that future mid-flight too (disposal-under-cancellation path).
                if !held.is_empty() {
                    let c = held.remove(0);
                    let delay_us = u64::from(op) % 100;
                    let fut = pool.release(c);
                    tokio::pin!(fut);
                    let _ = tokio::time::timeout(Duration::from_micros(delay_us), &mut fut).await;
                }
            }
        }
    }

    // Drain whatever is still held, then assert the pool's invariant: every connection the pool
    // thinks it owns is accounted for (idle + in_use == size) — i.e. no reserved slot or
    // briefly-borrowed connection leaked from a future dropped mid-acquire/mid-release.
    for c in held {
        let _ = pool.release(c).await;
    }
    pool.close(Some(1.0)).await;

    let stats = pool.stats();
    assert_eq!(
        stats.idle + stats.in_use,
        stats.size,
        "invariant violated: idle({}) + in_use({}) != size({})",
        stats.idle,
        stats.in_use,
        stats.size
    );
}
