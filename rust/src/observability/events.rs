//! `PoolListener`: observability hooks, and the internal dispatch group that isolates panics.

use std::error::Error;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::Arc;

use crate::core::connection::{CloseReason, PooledConnection};
use crate::resilience::breaker::BreakerState;

/// Observability hooks for metrics, tracing, logging. Implement the ones you care about; all
/// default to no-ops. Hooks run synchronously on the calling thread, outside the pool's lock,
/// so keep them fast. Panics raised by a listener are caught and logged — a broken metrics
/// exporter must never break the pool.
pub trait PoolListener<T>: Send + Sync {
    /// A new connection was created and registered.
    fn on_connection_created(&self, _conn: &PooledConnection<T>, _duration: f64) {}
    /// A connection was closed, with the reason it left the pool.
    fn on_connection_closed(&self, _conn: &PooledConnection<T>, _reason: CloseReason) {}
    /// A `create` attempt failed (fires per attempt inside the retry loop).
    fn on_create_failed(&self, _error: &(dyn Error + 'static)) {}
    /// A connection was acquired, with how long the caller waited.
    fn on_acquired(&self, _conn: &PooledConnection<T>, _wait_time: f64) {}
    /// A connection was released, with how long it was held.
    fn on_released(&self, _conn: &PooledConnection<T>, _hold_time: f64) {}
    /// An acquire timed out, with how long the caller waited.
    fn on_acquire_timeout(&self, _waited: f64) {}
    /// A borrowed connection was flagged as possibly leaked (held too long).
    fn on_connection_leak(&self, _conn: &PooledConnection<T>, _held_for: f64) {}
    /// The circuit breaker changed state.
    fn on_breaker_state_change(&self, _old: BreakerState, _new: BreakerState) {}
}

/// An internal group of listeners with panic-isolating dispatch.
pub(crate) struct Listeners<T> {
    listeners: Vec<Arc<dyn PoolListener<T>>>,
}

impl<T> Listeners<T> {
    pub(crate) fn new(listeners: Vec<Arc<dyn PoolListener<T>>>) -> Self {
        Self { listeners }
    }

    /// Calls `f` for every listener, catching and logging any panic.
    pub(crate) fn safe<F>(&self, f: F)
    where
        F: Fn(&dyn PoolListener<T>),
    {
        for listener in &self.listeners {
            let l = listener.as_ref();
            if catch_unwind(AssertUnwindSafe(|| f(l))).is_err() {
                eprintln!("connpool: a pool listener panicked (ignored)");
            }
        }
    }

    /// Dispatches a breaker state change to every listener (a named entry point so the pool's
    /// breaker callback is a plain call rather than a nested closure).
    pub(crate) fn breaker_change(&self, old: BreakerState, new: BreakerState) {
        self.safe(|l| l.on_breaker_state_change(old, new));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    struct Counting(Arc<AtomicU32>);
    impl PoolListener<u32> for Counting {
        fn on_acquire_timeout(&self, _waited: f64) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct Panicky;
    impl PoolListener<u32> for Panicky {
        fn on_acquire_timeout(&self, _waited: f64) {
            panic!("boom");
        }
    }

    #[test]
    fn default_hooks_are_noops() {
        struct Noop;
        impl PoolListener<u32> for Noop {}
        let n = Noop;
        let pc = PooledConnection::new(1u32, 0.0, None);
        // Exercise every default method.
        n.on_connection_created(&pc, 1.0);
        n.on_connection_closed(&pc, CloseReason::Idle);
        let e: Box<dyn Error + Send + Sync> = "x".into();
        n.on_create_failed(&*e);
        n.on_acquired(&pc, 1.0);
        n.on_released(&pc, 1.0);
        n.on_acquire_timeout(1.0);
        n.on_connection_leak(&pc, 1.0);
        n.on_breaker_state_change(BreakerState::Closed, BreakerState::Open);
    }

    #[test]
    fn dispatch_isolates_panics_and_reaches_all() {
        let count = Arc::new(AtomicU32::new(0));
        let group: Listeners<u32> =
            Listeners::new(vec![Arc::new(Panicky), Arc::new(Counting(count.clone()))]);
        group.safe(|l| l.on_acquire_timeout(2.0));
        // The counting listener still ran despite the panicky one.
        assert_eq!(count.load(Ordering::SeqCst), 1);
    }
}
