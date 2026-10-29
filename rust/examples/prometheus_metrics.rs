//! Renders a `PoolStats` snapshot in the Prometheus text exposition format (blocking pool,
//! std only). Run with:
//!
//! ```text
//! cargo run --example prometheus_metrics
//! ```
//!
//! In a real service you would serve `render()` from a `/metrics` endpoint.

use std::error::Error;
use std::fmt::Write;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use connpool::{
    ConnectionPool, FactoryError, FnFactory, PoolConfig, PoolOptions, PoolStats, Timeout,
};

type Conn = Arc<u64>;

/// Escapes a label value per the Prometheus text format.
fn esc(v: &str) -> String {
    v.replace('\\', "\\\\").replace('"', "\\\"").replace('\n', "\\n")
}

/// Appends one metric (HELP + TYPE + sample lines). `samples` are (label-set, value).
fn metric(out: &mut String, name: &str, kind: &str, help: &str, samples: &[(String, f64)]) {
    let _ = writeln!(out, "# HELP {name} {help}\n# TYPE {name} {kind}");
    for (labels, v) in samples {
        let _ = writeln!(out, "{name}{{{labels}}} {v}");
    }
}

fn render(s: &PoolStats) -> String {
    let pool = format!("pool=\"{}\"", esc(&s.name));
    let one = |v: f64| vec![(pool.clone(), v)];
    let mut out = String::new();

    for (n, help, v) in [
        ("max_size", "Configured hard cap.", s.max_size),
        ("size", "Open connections.", s.size),
        ("idle", "Idle connections.", s.idle),
        ("in_use", "Checked-out connections.", s.in_use),
        ("waiting", "Threads waiting to acquire.", s.waiting),
    ] {
        metric(&mut out, &format!("connpool_{n}"), "gauge", help, &one(v as f64));
    }
    for (n, help, v) in [
        ("created_total", "Connections created.", s.created),
        ("create_failures_total", "Failed creates.", s.create_failures),
        ("acquired_total", "Successful acquires.", s.acquired),
        ("released_total", "Releases.", s.released),
        ("timeouts_total", "Acquire timeouts.", s.timeouts),
        ("closed_total", "Connections closed.", s.closed),
        ("leaks_total", "Leaks flagged.", s.leaks),
    ] {
        metric(&mut out, &format!("connpool_{n}"), "counter", help, &one(v as f64));
    }

    let by_reason: Vec<_> = s
        .closed_by_reason
        .iter()
        .map(|(r, c)| (format!("{pool},reason=\"{}\"", esc(r)), *c as f64))
        .collect();
    metric(&mut out, "connpool_closed_by_reason_total", "counter", "Closes by reason.", &by_reason);

    let q = |p: f64, v: f64| (format!("{pool},quantile=\"{p}\""), v);
    metric(
        &mut out,
        "connpool_acquire_wait_seconds",
        "gauge",
        "Acquire wait quantiles (recent window).",
        &[q(0.5, s.p50_acquire_wait), q(0.95, s.p95_acquire_wait), q(0.99, s.p99_acquire_wait)],
    );

    if let Some(state) = &s.breaker_state {
        let st = vec![(format!("{pool},state=\"{}\"", esc(state)), 1.0)];
        metric(&mut out, "connpool_breaker_state", "gauge", "Current breaker state.", &st);
    }
    out
}

fn main() -> Result<(), Box<dyn Error>> {
    let next = Arc::new(AtomicU64::new(0));
    let factory = FnFactory::new({
        let next = next.clone();
        move || Ok::<_, FactoryError>(Arc::new(next.fetch_add(1, Ordering::SeqCst)))
    })
    // Treat I/O errors reported by callers as proof the connection is broken.
    .with_is_fatal_error(|e| e.is::<io::Error>());

    let config = PoolConfig {
        max_size: 2,
        maintenance_interval: None,
        ..Default::default()
    };
    let pool = ConnectionPool::<Conn>::new(
        factory,
        config,
        Some(PoolOptions {
            name: Some("orders-db".into()),
            ..Default::default()
        }),
    )?;

    // Normal use.
    let a = pool.acquire(Timeout::Unset)?;
    pool.release(a)?;

    // Caller work fails: release is explicit and still happens; the factory deems it fatal.
    let b = pool.acquire(Timeout::Unset)?;
    let work: Result<(), io::Error> =
        Err(io::Error::new(io::ErrorKind::BrokenPipe, "socket reset"));
    match work {
        Ok(()) => pool.release(b)?,
        Err(e) => pool.release_with_error(b, &e)?,
    }

    // Exhaust the pool to record a timeout.
    let c = pool.acquire(Timeout::Unset)?;
    let d = pool.acquire(Timeout::Unset)?;
    let timed_out = pool.acquire(Timeout::Of(0.05));
    println!("third acquire failed as expected: {}\n", timed_out.is_err());
    pool.release(c)?;
    pool.release(d)?;

    print!("{}", render(&pool.stats()));
    pool.close(None);
    Ok(())
}
