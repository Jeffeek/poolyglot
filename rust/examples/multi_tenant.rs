//! One pool per tenant via `PoolRegistry` (blocking, std only). Run with:
//!
//! ```text
//! cargo run --example multi_tenant
//! ```
//!
//! The registry builds a pool lazily per key, using the factory returned for that key.
//! Tenants are isolated: one exhausting its pool does not affect the others.

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use connpool::{ConnectionFactory, FactoryError, FnFactory, PoolConfig, PoolRegistry, Timeout};

/// A toy connection: "<tenant>-<n>", unique within its tenant's pool.
type Conn = Arc<String>;

fn main() -> Result<(), Box<dyn Error>> {
    let registry: PoolRegistry<String, Conn> = PoolRegistry::new(
        |tenant: &String| {
            let tenant = tenant.clone();
            let next = AtomicU64::new(0);
            Box::new(FnFactory::new(move || {
                Ok::<_, FactoryError>(Arc::new(format!(
                    "{tenant}-{}",
                    next.fetch_add(1, Ordering::SeqCst)
                )))
            })) as Box<dyn ConnectionFactory<Conn>>
        },
        PoolConfig {
            max_size: 1,
            maintenance_interval: None,
            ..Default::default()
        },
    );

    let acme = "acme".to_string();
    let globex = "globex".to_string();

    // Hold acme's only connection (explicit acquire/release, as Rust has no lease wrapper).
    let pool_acme = registry.pool(&acme)?;
    let held = pool_acme.acquire(Timeout::Unset)?;
    println!("acme holds {held}");

    // acme is exhausted, but globex has its own pool and is unaffected.
    let blocked = pool_acme.acquire(Timeout::Of(0.05));
    println!("acme second acquire failed: {}", blocked.is_err());
    let used = registry.with_connection(&globex, Timeout::Of(1.0), |c| c.to_string())?;
    println!("globex used {used}");

    pool_acme.release(held)?;

    // Same key returns the same pool, so the released connection is reused.
    let again = registry.with_connection(&acme, Timeout::Of(1.0), |c| c.to_string())?;
    println!("acme reused {again}");

    for t in [&acme, &globex] {
        let s = registry.pool(t)?.stats();
        println!("{}: created={} acquired={} released={}", s.name, s.created, s.acquired, s.released);
    }

    registry.close();
    println!("registry closed; pool() now errs: {}", registry.pool(&acme).is_err());
    Ok(())
}
