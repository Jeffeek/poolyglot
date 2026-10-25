//! Async test fakes (feature `tokio`): a delay-capable async factory and a harness that wires
//! an [`AsyncConnectionPool`] to a controllable clock. Reuses [`Conn`] and [`Recorder`] from the
//! sync test support.

#![allow(dead_code)]

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::core::clock::{Clock, ManualClock, SystemClock};
use crate::core::errors::FactoryError;
use crate::factories::async_factory::AsyncConnectionFactory;
use crate::observability::events::PoolListener;
use crate::policy::config::PoolConfig;
use crate::pools::async_pool::AsyncConnectionPool;
use crate::pools::sync_pool::PoolOptions;
use crate::rng::SystemRng;

/// Shared, mutable async-factory behavior.
pub struct AsyncFactoryState {
    next_id: AtomicU64,
    pub created: AtomicU64,
    closed: Mutex<Vec<u64>>,
    validate_ok: AtomicBool,
    reset_ok: AtomicBool,
    fatal: AtomicBool,
    fail_creates: AtomicU64,
    force_id: Mutex<Option<u64>>,
    create_delay_ms: AtomicU64,
    validate_delay_ms: AtomicU64,
    close_err: AtomicBool,
}

impl AsyncFactoryState {
    pub fn new() -> Arc<Self> {
        Arc::new(AsyncFactoryState {
            next_id: AtomicU64::new(1),
            created: AtomicU64::new(0),
            closed: Mutex::new(Vec::new()),
            validate_ok: AtomicBool::new(true),
            reset_ok: AtomicBool::new(true),
            fatal: AtomicBool::new(false),
            fail_creates: AtomicU64::new(0),
            force_id: Mutex::new(None),
            create_delay_ms: AtomicU64::new(0),
            validate_delay_ms: AtomicU64::new(0),
            close_err: AtomicBool::new(false),
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
    pub fn set_create_delay_ms(&self, ms: u64) {
        self.create_delay_ms.store(ms, Ordering::SeqCst);
    }
    pub fn set_validate_delay_ms(&self, ms: u64) {
        self.validate_delay_ms.store(ms, Ordering::SeqCst);
    }
    pub fn set_close_err(&self, e: bool) {
        self.close_err.store(e, Ordering::SeqCst);
    }
    pub fn closed_ids(&self) -> Vec<u64> {
        self.closed.lock().unwrap().clone()
    }
}

/// An [`AsyncConnectionFactory`] over a shared [`AsyncFactoryState`].
pub struct AsyncTestFactory(pub Arc<AsyncFactoryState>);

impl AsyncConnectionFactory<Conn> for AsyncTestFactory {
    async fn create(&self) -> Result<Conn, FactoryError> {
        let s = &self.0;
        let delay = s.create_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
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

    async fn validate(&self, _conn: &Conn) -> bool {
        let delay = self.0.validate_delay_ms.load(Ordering::SeqCst);
        if delay > 0 {
            tokio::time::sleep(Duration::from_millis(delay)).await;
        }
        self.0.validate_ok.load(Ordering::SeqCst)
    }

    async fn reset(&self, _conn: &Conn) -> Result<(), FactoryError> {
        if self.0.reset_ok.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err("reset failed".into())
        }
    }

    async fn close(&self, conn: &Conn) -> Result<(), FactoryError> {
        self.0.closed.lock().unwrap().push(conn.id);
        if self.0.close_err.load(Ordering::SeqCst) {
            return Err("close failed".into());
        }
        Ok(())
    }

    fn is_fatal_error(&self, _err: &(dyn Error + 'static)) -> bool {
        self.0.fatal.load(Ordering::SeqCst)
    }
}

pub use crate::test_support::{Conn, Recorder};

/// Wires an async pool to a controllable clock and shared factory state.
pub struct AsyncHarness {
    pub clock: Arc<ManualClock>,
    pub fac: Arc<AsyncFactoryState>,
    system: bool,
}

impl AsyncHarness {
    pub fn new() -> Self {
        AsyncHarness {
            clock: Arc::new(ManualClock::new(1000.0)),
            fac: AsyncFactoryState::new(),
            system: false,
        }
    }

    /// A harness whose pool uses a real [`SystemClock`] (for delay/timeout tests).
    pub fn with_system_clock() -> Self {
        AsyncHarness {
            clock: Arc::new(ManualClock::new(1000.0)),
            fac: AsyncFactoryState::new(),
            system: true,
        }
    }

    pub fn factory(&self) -> AsyncTestFactory {
        AsyncTestFactory(self.fac.clone())
    }

    fn options(&self, listeners: Vec<Arc<dyn PoolListener<Conn>>>) -> PoolOptions<Conn> {
        let clock: Arc<dyn Clock> = if self.system {
            Arc::new(SystemClock::new())
        } else {
            self.clock.clone()
        };
        PoolOptions {
            name: Some("test".to_string()),
            clock: Some(clock),
            rng: Some(Box::new(SystemRng::with_seed(1))),
            listeners,
        }
    }

    pub fn pool(&self, cfg: PoolConfig) -> AsyncConnectionPool<Conn, AsyncTestFactory> {
        AsyncConnectionPool::new(self.factory(), cfg, Some(self.options(vec![]))).unwrap()
    }

    pub fn pool_with(
        &self,
        cfg: PoolConfig,
        listeners: Vec<Arc<dyn PoolListener<Conn>>>,
    ) -> AsyncConnectionPool<Conn, AsyncTestFactory> {
        AsyncConnectionPool::new(self.factory(), cfg, Some(self.options(listeners))).unwrap()
    }
}
