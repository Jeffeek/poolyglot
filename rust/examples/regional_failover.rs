//! Failover across per-region pools (blocking, std only). Run with:
//!
//! ```text
//! cargo run --example regional_failover
//! ```
//!
//! Each region has its own pool (and circuit breaker). A request tries regions in order and
//! falls through when a region cannot hand out a connection. The connection is always
//! released, with the caller's error reported so the pool can discard a broken one.

use std::error::Error;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use connpool::{
    BreakerConfig, ConnectionPool, FactoryError, FnFactory, PoolConfig, PoolError, PoolOptions,
    RetryPolicy, Timeout,
};

type Conn = Arc<u64>;

/// Builds a region pool whose factory fails while `down` is set.
fn region(name: &str, down: Arc<AtomicBool>) -> Result<ConnectionPool<Conn>, PoolError> {
    let next = Arc::new(AtomicU64::new(0));
    let factory = FnFactory::new(move || {
        if down.load(Ordering::SeqCst) {
            return Err::<Conn, FactoryError>("region unreachable".into());
        }
        Ok(Arc::new(next.fetch_add(1, Ordering::SeqCst)))
    });
    let config = PoolConfig {
        max_size: 2,
        maintenance_interval: None,
        retry: RetryPolicy::no_retry(),
        breaker: Some(BreakerConfig {
            failure_threshold: 2,
            ..Default::default()
        }),
        ..Default::default()
    };
    ConnectionPool::new(
        factory,
        config,
        Some(PoolOptions {
            name: Some(name.into()),
            ..Default::default()
        }),
    )
}

/// Runs `work` on the first region that can supply a connection; returns that region's name.
fn with_failover(
    regions: &[ConnectionPool<Conn>],
    work: impl Fn(&Conn) -> Result<(), io::Error>,
) -> Result<&str, Box<dyn Error>> {
    for pool in regions {
        let conn = match pool.acquire(Timeout::Of(1.0)) {
            Ok(c) => c,
            Err(
                e @ (PoolError::ConnectionCreate { .. }
                | PoolError::CircuitOpen { .. }
                | PoolError::Timeout(_)
                | PoolError::Exhausted(_)),
            ) => {
                println!("  {} unavailable ({e}), trying next", pool.name());
                continue;
            }
            Err(e) => return Err(e.into()),
        };
        // Release exactly once on every path.
        return match work(&conn) {
            Ok(()) => {
                pool.release(conn)?;
                Ok(pool.name())
            }
            Err(e) => {
                pool.release_with_error(conn, &e)?;
                Err(e.into())
            }
        };
    }
    Err("all regions unavailable".into())
}

fn main() -> Result<(), Box<dyn Error>> {
    let primary_down = Arc::new(AtomicBool::new(false));
    let regions = [
        region("us-east", primary_down.clone())?,
        region("eu-west", Arc::new(AtomicBool::new(false)))?,
    ];
    let work = |c: &Conn| {
        println!("  working on connection {}", **c);
        Ok(())
    };

    println!("primary healthy:");
    println!("  served by {}", with_failover(&regions, work)?);

    // Lose the primary: drop its warm connection so the next acquire must create (and fail).
    primary_down.store(true, Ordering::SeqCst);
    let warm = regions[0].acquire(Timeout::Unset)?;
    regions[0].discard(warm)?;

    for i in 1..=3 {
        println!("primary down, request {i}:");
        println!("  served by {}", with_failover(&regions, work)?);
    }

    for p in &regions {
        let s = p.stats();
        println!(
            "{}: breaker={:?} create_failures={} acquired={}",
            s.name, s.breaker_state, s.create_failures, s.acquired
        );
        p.close(None);
    }
    Ok(())
}
