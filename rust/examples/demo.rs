//! A runnable demo of the blocking pool (zero dependencies). Run with:
//!
//! ```text
//! cargo run --example demo
//! ```
//!
//! It pools a toy "connection" (a unique id behind an `Arc`, so clones share identity) and
//! shows acquire/reuse, a listener, and a stats snapshot. For a real backend you implement
//! `ConnectionFactory` over your driver instead of this `FnFactory`.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use connpool::{
    CloseReason, ConnectionPool, FactoryError, FnFactory, PoolConfig, PoolListener,
    PooledConnection, Timeout,
};

/// Our pooled "connection": a cheap, clonable, identity-bearing handle.
type Conn = Arc<u64>;

/// A listener that just prints lifecycle events.
struct Printer;

impl PoolListener<Conn> for Printer {
    fn on_connection_created(&self, conn: &PooledConnection<Conn>, duration: f64) {
        println!("  + created #{} in {:.4}s", conn.id(), duration);
    }
    fn on_acquired(&self, conn: &PooledConnection<Conn>, wait: f64) {
        println!("  -> acquired #{} (waited {:.4}s)", conn.id(), wait);
    }
    fn on_released(&self, conn: &PooledConnection<Conn>, held: f64) {
        println!("  <- released #{} (held {:.4}s)", conn.id(), held);
    }
    fn on_connection_closed(&self, conn: &PooledConnection<Conn>, reason: CloseReason) {
        println!("  x closed #{} ({})", conn.id(), reason.as_str());
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    // The factory: hand out a fresh id each create(), wrapped in an Arc so the pool and the
    // caller share the same underlying "connection".
    let next = Arc::new(AtomicU64::new(0));
    let factory = FnFactory::new({
        let next = next.clone();
        move || Ok::<_, FactoryError>(Arc::new(next.fetch_add(1, Ordering::SeqCst)))
    });

    let config = PoolConfig {
        min_size: 1,
        max_size: 4,
        ..Default::default()
    };

    let pool = ConnectionPool::new(factory, config, None)?;
    pool.open()?; // warm up min_size and start background maintenance

    println!("acquire two, release both, re-acquire (LIFO reuse):");
    let a = pool.acquire(Timeout::Unset)?;
    let b = pool.acquire(Timeout::Unset)?;
    println!("  got {} and {}", *a, *b);
    pool.release(a)?;
    pool.release(b)?;

    // Attach a listener via options for the richer demo below.
    let next2 = Arc::new(AtomicU64::new(100));
    let listed = ConnectionPool::new(
        FnFactory::new({
            let next2 = next2.clone();
            move || Ok::<_, FactoryError>(Arc::new(next2.fetch_add(1, Ordering::SeqCst)))
        }),
        PoolConfig { max_size: 2, ..Default::default() },
        Some(connpool::PoolOptions {
            name: Some("demo".into()),
            clock: None,
            rng: None,
            listeners: vec![Arc::new(Printer)],
        }),
    )?;

    println!("\nwith a listener attached:");
    listed.with_connection(Timeout::Unset, |c| {
        println!("  used connection {}", **c);
    })?;

    let stats = listed.stats();
    println!(
        "\nstats: size={} idle={} created={} acquired={} released={}",
        stats.size, stats.idle, stats.created, stats.acquired, stats.released
    );

    pool.close(Some(1.0));
    listed.close(None);
    println!("\ndone.");
    Ok(())
}
