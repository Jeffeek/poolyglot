//! Shared test fakes: a deterministic connection type, a configurable factory, a recording
//! listener, and a harness that wires a pool to a [`ManualClock`] + seeded RNG.

#![allow(dead_code)]

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::core::clock::ManualClock;
use crate::core::connection::{CloseReason, PooledConnection};
use crate::core::errors::FactoryError;
use crate::factories::sync_factory::ConnectionFactory;
use crate::observability::events::PoolListener;
use crate::policy::config::PoolConfig;
use crate::pools::sync_pool::{ConnectionPool, PoolOptions};
use crate::resilience::breaker::BreakerState;
use crate::rng::SystemRng;

/// A cheap connection handle: identity is its `id`. Clones with the same id are "the same
/// connection" (the pool tracks by value identity).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Conn {
    pub id: u64,
}

impl Conn {
    pub fn new(id: u64) -> Self {
        Conn { id }
    }
}

/// Shared, mutable factory behavior so tests can steer create/validate/reset/close outcomes.
pub struct FactoryState {
    next_id: AtomicU64,
    pub created: AtomicU64,
    pub closed: Mutex<Vec<u64>>,
    pub reset: Mutex<Vec<u64>>,
    pub validated: Mutex<Vec<u64>>,
    validate_ok: AtomicBool,
    reset_ok: AtomicBool,
    fatal: AtomicBool,
    /// Number of upcoming create calls that should fail.
    fail_creates: AtomicU64,
    /// If set, create returns a connection with this fixed id (to test duplicate rejection).
    force_id: Mutex<Option<u64>>,
    create_panic: AtomicBool,
    validate_panic: AtomicBool,
    reset_panic: AtomicBool,
    close_err: AtomicBool,
    close_panic: AtomicBool,
}

impl FactoryState {
    pub fn new() -> Arc<Self> {
        Arc::new(FactoryState {
            next_id: AtomicU64::new(1),
            created: AtomicU64::new(0),
            closed: Mutex::new(Vec::new()),
            reset: Mutex::new(Vec::new()),
            validated: Mutex::new(Vec::new()),
            validate_ok: AtomicBool::new(true),
            reset_ok: AtomicBool::new(true),
            fatal: AtomicBool::new(false),
            fail_creates: AtomicU64::new(0),
            force_id: Mutex::new(None),
            create_panic: AtomicBool::new(false),
            validate_panic: AtomicBool::new(false),
            reset_panic: AtomicBool::new(false),
            close_err: AtomicBool::new(false),
            close_panic: AtomicBool::new(false),
        })
    }

    pub fn set_validate_ok(&self, ok: bool) {
        self.validate_ok.store(ok, Ordering::SeqCst);
    }
    pub fn set_reset_ok(&self, ok: bool) {
        self.reset_ok.store(ok, Ordering::SeqCst);
    }
    pub fn set_fatal(&self, f: bool) {
        self.fatal.store(f, Ordering::SeqCst);
    }
    pub fn fail_next_creates(&self, n: u64) {
        self.fail_creates.store(n, Ordering::SeqCst);
    }
    pub fn force_id(&self, id: Option<u64>) {
        *self.force_id.lock().unwrap() = id;
    }
    pub fn set_create_panic(&self, p: bool) {
        self.create_panic.store(p, Ordering::SeqCst);
    }
    pub fn set_validate_panic(&self, p: bool) {
        self.validate_panic.store(p, Ordering::SeqCst);
    }
    pub fn set_reset_panic(&self, p: bool) {
        self.reset_panic.store(p, Ordering::SeqCst);
    }
    pub fn set_close_err(&self, e: bool) {
        self.close_err.store(e, Ordering::SeqCst);
    }
    pub fn set_close_panic(&self, p: bool) {
        self.close_panic.store(p, Ordering::SeqCst);
    }
    pub fn closed_ids(&self) -> Vec<u64> {
        self.closed.lock().unwrap().clone()
    }
}

/// A [`ConnectionFactory`] over a shared [`FactoryState`].
pub struct TestFactory(pub Arc<FactoryState>);

impl ConnectionFactory<Conn> for TestFactory {
    fn create(&self) -> Result<Conn, FactoryError> {
        let s = &self.0;
        if s.create_panic.load(Ordering::SeqCst) {
            panic!("create boom");
        }
        if s.fail_creates.load(Ordering::SeqCst) > 0 {
            s.fail_creates.fetch_sub(1, Ordering::SeqCst);
            return Err("create failed".into());
        }
        s.created.fetch_add(1, Ordering::SeqCst);
        let id = match *s.force_id.lock().unwrap() {
            Some(id) => id,
            None => s.next_id.fetch_add(1, Ordering::SeqCst),
        };
        Ok(Conn::new(id))
    }

    fn validate(&self, conn: &Conn) -> bool {
        let s = &self.0;
        if s.validate_panic.load(Ordering::SeqCst) {
            panic!("validate boom");
        }
        s.validated.lock().unwrap().push(conn.id);
        s.validate_ok.load(Ordering::SeqCst)
    }

    fn reset(&self, conn: &Conn) -> Result<(), FactoryError> {
        let s = &self.0;
        if s.reset_panic.load(Ordering::SeqCst) {
            panic!("reset boom");
        }
        s.reset.lock().unwrap().push(conn.id);
        if s.reset_ok.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err("reset failed".into())
        }
    }

    fn close(&self, conn: &Conn) -> Result<(), FactoryError> {
        self.0.closed.lock().unwrap().push(conn.id);
        if self.0.close_panic.load(Ordering::SeqCst) {
            panic!("close boom");
        }
        if self.0.close_err.load(Ordering::SeqCst) {
            return Err("close failed".into());
        }
        Ok(())
    }

    fn is_fatal_error(&self, _err: &(dyn Error + 'static)) -> bool {
        self.0.fatal.load(Ordering::SeqCst)
    }
}

/// A listener that records every event for assertions.
#[derive(Default)]
pub struct Recorder {
    pub created: AtomicU64,
    pub closed: AtomicU64,
    pub create_failed: AtomicU64,
    pub acquired: AtomicU64,
    pub released: AtomicU64,
    pub timeouts: AtomicU64,
    pub leaks: AtomicU64,
    pub breaker_changes: Mutex<Vec<(BreakerState, BreakerState)>>,
}

impl Recorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Recorder::default())
    }
}

impl PoolListener<Conn> for Recorder {
    fn on_connection_created(&self, _c: &PooledConnection<Conn>, _d: f64) {
        self.created.fetch_add(1, Ordering::SeqCst);
    }
    fn on_connection_closed(&self, _c: &PooledConnection<Conn>, _r: CloseReason) {
        self.closed.fetch_add(1, Ordering::SeqCst);
    }
    fn on_create_failed(&self, _e: &(dyn Error + 'static)) {
        self.create_failed.fetch_add(1, Ordering::SeqCst);
    }
    fn on_acquired(&self, _c: &PooledConnection<Conn>, _w: f64) {
        self.acquired.fetch_add(1, Ordering::SeqCst);
    }
    fn on_released(&self, _c: &PooledConnection<Conn>, _h: f64) {
        self.released.fetch_add(1, Ordering::SeqCst);
    }
    fn on_acquire_timeout(&self, _w: f64) {
        self.timeouts.fetch_add(1, Ordering::SeqCst);
    }
    fn on_connection_leak(&self, _c: &PooledConnection<Conn>, _h: f64) {
        self.leaks.fetch_add(1, Ordering::SeqCst);
    }
    fn on_breaker_state_change(&self, old: BreakerState, new: BreakerState) {
        self.breaker_changes.lock().unwrap().push((old, new));
    }
}

/// Wires a pool to a controllable clock, seeded RNG and shared factory state.
pub struct Harness {
    pub clock: Arc<ManualClock>,
    pub fac: Arc<FactoryState>,
}

impl Harness {
    pub fn new() -> Self {
        Harness {
            clock: Arc::new(ManualClock::new(1000.0)),
            fac: FactoryState::new(),
        }
    }

    pub fn factory(&self) -> TestFactory {
        TestFactory(self.fac.clone())
    }

    fn options(&self, listeners: Vec<Arc<dyn PoolListener<Conn>>>) -> PoolOptions<Conn> {
        PoolOptions {
            name: Some("test".to_string()),
            clock: Some(self.clock.clone()),
            rng: Some(Box::new(SystemRng::with_seed(1))),
            listeners,
        }
    }

    pub fn pool(&self, cfg: PoolConfig) -> ConnectionPool<Conn> {
        ConnectionPool::new(self.factory(), cfg, Some(self.options(vec![]))).unwrap()
    }

    pub fn pool_with(
        &self,
        cfg: PoolConfig,
        listeners: Vec<Arc<dyn PoolListener<Conn>>>,
    ) -> ConnectionPool<Conn> {
        ConnectionPool::new(self.factory(), cfg, Some(self.options(listeners))).unwrap()
    }
}
