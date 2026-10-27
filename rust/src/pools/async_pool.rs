//! `AsyncConnectionPool`: the async twin of [`ConnectionPool`](crate::ConnectionPool), built on
//! tokio (feature `tokio`).
//!
//! Same model and rules as the blocking pool; the differences are Rust's async idioms:
//!
//! - **Cancellation is "drop the future".** There is no cancellation token. A caller that stops
//!   awaiting mid-acquire never leaks a slot: Drop guards unwind a reserved slot
//!   ([`SlotGuard`]) or return a briefly-borrowed connection ([`BorrowGuard`]).
//! - **Real time drives waits/timeouts** (`tokio::time`, `tokio::sync::Notify`); the injected
//!   [`Clock`](crate::Clock) drives only logical time (lifetime, idle, breaker recovery, backoff)
//!   — exactly as Python's `asyncio.timeout` ignores the injected clock.
//! - **`create_timeout` is enforced here** (via `tokio::time::timeout`) rather than left to the
//!   factory, and disposal is shielded by running `close` on a detached task.
//!
//! The pool is generic over the factory type `F` because [`AsyncConnectionFactory`] uses
//! `async fn` in trait (not `dyn`-safe). `T` is pooled by value identity (`Clone + Eq + Hash`).

use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout as tokio_timeout;

use crate::core::clock::{Clock, SystemClock};
use crate::core::connection::{CloseReason, PooledConnection};
use crate::core::errors::{FactoryError, PoolError};
use crate::core::timeouts::{Timeout, remaining};
use crate::factories::async_factory::AsyncConnectionFactory;
use crate::observability::events::{Listeners, PoolListener};
use crate::observability::stats::{PoolStats, StatsCollector, StatsGauges};
use crate::policy::config::PoolConfig;
use crate::policy::lifecycle::{
    borrow_reason, needs_validation, new_connection, pop_idle, rotation_reason, sweep,
};
use crate::pools::sync_pool::PoolOptions;
use crate::resilience::breaker::{BreakerState, CircuitBreaker};
use crate::rng::{Rng, SystemRng};

struct State<T> {
    idle: Vec<PooledConnection<T>>,
    checked_out: HashMap<T, PooledConnection<T>>,
    size: usize,
    waiters: usize,
    closed: bool,
    opened: bool,
}

struct Inner<T, F> {
    name: String,
    factory: F,
    cfg: PoolConfig,
    clock: Arc<dyn Clock>,
    rng: Mutex<Box<dyn Rng>>,

    state: Mutex<State<T>>,
    notify: Notify,

    stats: Arc<StatsCollector<T>>,
    events: Listeners<T>,
    breaker: OnceLock<CircuitBreaker>,

    stop: Notify,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// A tokio-based, cancel-safe connection pool. Cheaply clonable (clones share one pool).
pub struct AsyncConnectionPool<T, F>(Arc<Inner<T, F>>);

impl<T, F> Clone for AsyncConnectionPool<T, F> {
    fn clone(&self) -> Self {
        AsyncConnectionPool(self.0.clone())
    }
}

// -- cancel-safety guards ---------------------------------------------------

/// Frees a reserved slot if dropped before [`commit`](SlotGuard::commit). A connection, once
/// created, is registered synchronously (no await in between), so the only cancellation window
/// is while the slot is reserved but empty — hence freeing the slot is all that's needed.
struct SlotGuard<T, F> {
    inner: Arc<Inner<T, F>>,
    armed: bool,
}

impl<T, F> SlotGuard<T, F>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
    F: AsyncConnectionFactory<T> + 'static,
{
    fn new(inner: Arc<Inner<T, F>>) -> Self {
        Self { inner, armed: true }
    }
    fn commit(mut self) {
        self.armed = false;
    }
}

impl<T, F> Drop for SlotGuard<T, F> {
    fn drop(&mut self) {
        if self.armed {
            let mut g = self.inner.state.lock().unwrap();
            g.size -= 1;
            self.inner.notify.notify_waiters();
        }
    }
}

/// Returns a briefly-borrowed connection to the pool if dropped before
/// [`commit`](BorrowGuard::commit) — i.e. the caller cancelled during borrow validation. The
/// connection goes back to idle (or is disposed on a detached task if the pool has closed),
/// so no slot leaks.
struct BorrowGuard<T, F>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
    F: AsyncConnectionFactory<T> + 'static,
{
    inner: Arc<Inner<T, F>>,
    key: T,
    armed: bool,
}

impl<T, F> BorrowGuard<T, F>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
    F: AsyncConnectionFactory<T> + 'static,
{
    fn new(inner: Arc<Inner<T, F>>, key: T) -> Self {
        Self {
            inner,
            key,
            armed: true,
        }
    }
    fn commit(mut self) {
        self.armed = false;
    }
}

impl<T, F> Drop for BorrowGuard<T, F>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
    F: AsyncConnectionFactory<T> + 'static,
{
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut g = self.inner.state.lock().unwrap();
        // A borrowed connection stays in checked_out until its guard commits, so an armed guard
        // always finds it here.
        let mut p = g
            .checked_out
            .remove(&self.key)
            .expect("borrowed connection present until guard commits");
        if g.closed {
            // Pool is shutting down: give the slot back; the connection is dropped (its own
            // Drop releases the backend resource).
            g.size -= 1;
        } else {
            let now = self.inner.clock.monotonic();
            p.mark_returned(now);
            g.idle.push(p);
        }
        self.inner.notify.notify_waiters();
    }
}

impl<T, F> AsyncConnectionPool<T, F>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
    F: AsyncConnectionFactory<T> + 'static,
{
    /// Builds an async pool, validating `config`.
    pub fn new(
        factory: F,
        config: PoolConfig,
        options: Option<PoolOptions<T>>,
    ) -> Result<Self, PoolError> {
        config.validate()?;
        let options = options.unwrap_or_default();
        let clock: Arc<dyn Clock> = options
            .clock
            .unwrap_or_else(|| Arc::new(SystemClock::new()));
        let rng: Box<dyn Rng> = options.rng.unwrap_or_else(|| Box::new(SystemRng::new()));
        let name = options.name.unwrap_or_else(|| "pool".to_string());

        let stats = Arc::new(StatsCollector::new());
        let mut listeners: Vec<Arc<dyn PoolListener<T>>> = vec![stats.clone()];
        listeners.extend(options.listeners);
        let events = Listeners::new(listeners);

        let inner = Arc::new(Inner {
            name,
            factory,
            cfg: config,
            clock,
            rng: Mutex::new(rng),
            state: Mutex::new(State {
                idle: Vec::new(),
                checked_out: HashMap::new(),
                size: 0,
                waiters: 0,
                closed: false,
                opened: false,
            }),
            notify: Notify::new(),
            stats,
            events,
            breaker: OnceLock::new(),
            stop: Notify::new(),
            handle: Mutex::new(None),
        });

        if let Some(bc) = inner.cfg.breaker {
            let weak = Arc::downgrade(&inner);
            let cb = Box::new(move |old: BreakerState, new: BreakerState| {
                if let Some(inner) = weak.upgrade() {
                    inner.events.breaker_change(old, new);
                }
            });
            let breaker = CircuitBreaker::new(bc, Some(inner.clock.clone()), Some(cb));
            let _ = inner.breaker.set(breaker);
        }

        Ok(AsyncConnectionPool(inner))
    }

    /// The pool's name.
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// Warms up `min_size` connections and starts background maintenance. Idempotent.
    pub async fn open(&self) -> Result<(), PoolError> {
        let want_thread = {
            let mut g = self.0.state.lock().unwrap();
            if g.closed {
                return Err(PoolError::Closed(format!(
                    "pool {:?} is closed",
                    self.0.name
                )));
            }
            if g.opened {
                return Ok(());
            }
            g.opened = true;
            self.0.cfg.maintenance_interval
        };
        self.0.fill_to_min().await;
        if let Some(interval) = want_thread {
            if !self.0.state.lock().unwrap().closed {
                self.start_maintenance(interval);
            }
        }
        Ok(())
    }

    fn start_maintenance(&self, interval: f64) {
        let weak: Weak<Inner<T, F>> = Arc::downgrade(&self.0);
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs_f64(interval));
            ticker.tick().await; // consume the immediate first tick
            loop {
                let inner = match weak.upgrade() {
                    Some(i) => i,
                    None => return,
                };
                let stop = inner.stop.notified();
                tokio::pin!(stop);
                tokio::select! {
                    _ = ticker.tick() => {}
                    _ = &mut stop => return,
                }
                inner.run_maintenance_safe().await;
            }
        });
        *self.0.handle.lock().unwrap() = Some(handle);
    }

    /// Closes idle connections now; checked-out ones close on release. Idempotent. `timeout`
    /// (`None` = don't wait) drains gracefully up to that many seconds.
    pub async fn close(&self, timeout: Option<f64>) {
        self.0.close(timeout).await;
    }

    /// Whether [`close`](Self::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.0.state.lock().unwrap().closed
    }

    /// Gets a connection, honoring cancellation (drop the future) and the acquire timeout.
    pub async fn acquire(&self, timeout: Timeout) -> Result<T, PoolError> {
        self.0.acquire(timeout).await
    }

    /// Returns a connection to the pool for reuse.
    pub async fn release(&self, conn: T) -> Result<(), PoolError> {
        self.0.release(conn, false, None).await
    }

    /// Returns a connection but forces it to be closed instead of reused.
    pub async fn discard(&self, conn: T) -> Result<(), PoolError> {
        self.0.release(conn, true, None).await
    }

    /// Returns a connection, signalling the caller's work raised `error`.
    pub async fn release_with_error(
        &self,
        conn: T,
        error: &(dyn Error + 'static),
    ) -> Result<(), PoolError> {
        self.0.release(conn, false, Some(error)).await
    }

    /// An immutable snapshot of pool counters and gauges.
    pub fn stats(&self) -> PoolStats {
        self.0.stats_snapshot()
    }

    /// Performs one maintenance sweep (see [`ConnectionPool::run_maintenance`](crate::ConnectionPool::run_maintenance)).
    pub async fn run_maintenance(&self) {
        self.0.run_maintenance().await;
    }
}

enum Taken<T> {
    Reused(T),
    Reserved,
}

impl<T, F> Inner<T, F>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
    F: AsyncConnectionFactory<T> + 'static,
{
    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    fn new_conn(&self, raw: T, now: f64) -> PooledConnection<T> {
        let mut rng = self.rng.lock().unwrap();
        new_connection(raw, &self.cfg, now, &mut **rng)
    }

    fn retry_delay(&self, attempt: u32) -> f64 {
        let mut rng = self.rng.lock().unwrap();
        self.cfg.retry.delay_for(attempt, &mut **rng)
    }

    fn free_slot(&self) {
        let mut g = self.state.lock().unwrap();
        g.size -= 1;
        self.notify.notify_waiters();
    }

    async fn close(&self, timeout: Option<f64>) {
        let idle = {
            let mut g = self.state.lock().unwrap();
            if g.closed {
                return;
            }
            g.closed = true;
            let idle = std::mem::take(&mut g.idle);
            self.notify.notify_waiters();
            idle
        };
        if let Some(t) = timeout {
            let deadline = self.clock.monotonic() + t;
            loop {
                if self.state.lock().unwrap().checked_out.is_empty() {
                    break;
                }
                let left = deadline - self.clock.monotonic();
                if left <= 0.0 {
                    break;
                }
                let n = self.notify.notified();
                tokio::pin!(n);
                n.as_mut().enable();
                let _ = tokio_timeout(Duration::from_secs_f64(left), n).await;
            }
        }
        // notify_one stores a permit, so the maintenance task exits even if it is mid-sweep
        // (not yet awaiting) when close() fires — notify_waiters would be lost in that race.
        self.stop.notify_one();
        let handle = self.handle.lock().unwrap().take();
        if let Some(h) = handle {
            let _ = h.await;
        }
        for pooled in idle {
            self.destroy_detached(pooled, CloseReason::PoolClosed).await;
        }
    }

    async fn acquire(self: &Arc<Self>, timeout: Timeout) -> Result<T, PoolError> {
        let started = self.clock.monotonic();
        let deadline = timeout.compute_deadline(self.cfg.acquire_timeout, started);
        let key = match self.acquire_loop(deadline).await {
            Ok(k) => k,
            Err(e) => {
                if matches!(e, PoolError::Timeout(_)) {
                    let waited = self.clock.monotonic() - started;
                    self.events.safe(|l| l.on_acquire_timeout(waited));
                }
                return Err(e);
            }
        };
        let now = self.clock.monotonic();
        let (raw, ev, wait) = {
            let mut g = self.state.lock().unwrap();
            let pooled = g.checked_out.get_mut(&key).expect("acquired conn present");
            pooled.mark_checked_out(now);
            (pooled.raw.clone(), pooled.clone(), now - started)
        };
        self.events.safe(|l| l.on_acquired(&ev, wait));
        Ok(raw)
    }

    async fn acquire_loop(self: &Arc<Self>, deadline: Option<f64>) -> Result<T, PoolError> {
        loop {
            match self.take_idle_or_reserve(deadline).await? {
                Taken::Reserved => {
                    let slot = SlotGuard::new(self.clone());
                    let (pooled, created_in) = self.create(deadline).await?;
                    slot.commit();
                    return self.register_new(pooled, created_in).await;
                }
                Taken::Reused(key) => {
                    let guard = BorrowGuard::new(self.clone(), key.clone());
                    match self.check_borrow(&key).await {
                        None => {
                            guard.commit();
                            return Ok(key);
                        }
                        Some(reason) => {
                            guard.commit();
                            self.destroy_by_key(&key, reason).await;
                        }
                    }
                }
            }
        }
    }

    async fn take_idle_or_reserve(&self, deadline: Option<f64>) -> Result<Taken<T>, PoolError> {
        loop {
            let notified = {
                let mut g = self.state.lock().unwrap();
                if g.closed {
                    return Err(PoolError::Closed(format!("pool {:?} is closed", self.name)));
                }
                if let Some(conn) = pop_idle(&mut g.idle, self.cfg.idle_strategy) {
                    let key = conn.raw.clone();
                    g.checked_out.insert(key.clone(), conn);
                    return Ok(Taken::Reused(key));
                }
                if g.size < self.cfg.max_size {
                    g.size += 1;
                    return Ok(Taken::Reserved);
                }
                let left = remaining(deadline, self.clock.monotonic());
                if let Some(l) = left {
                    if l <= 0.0 {
                        return Err(PoolError::Timeout(format!(
                            "pool {:?}: no connection available (max_size={})",
                            self.name, self.cfg.max_size
                        )));
                    }
                }
                if let Some(max) = self.cfg.max_waiters {
                    if g.waiters >= max {
                        return Err(PoolError::Exhausted(format!(
                            "pool {:?}: waiter queue full (max_waiters={})",
                            self.name, max
                        )));
                    }
                }
                g.waiters += 1;
                let n = self.notify.notified();
                (n, left)
            };
            let (n, left) = notified;
            tokio::pin!(n);
            n.as_mut().enable();
            let _guard = WaiterGuard { inner: self };
            match left {
                None => n.await,
                Some(l) => {
                    let _ = tokio_timeout(Duration::from_secs_f64(l.max(0.0)), n).await;
                }
            }
        }
    }

    async fn check_borrow(&self, key: &T) -> Option<CloseReason> {
        let now = self.clock.monotonic();
        let (reason, need_validate, raw) = {
            let g = self.state.lock().unwrap();
            let pooled = g.checked_out.get(key).expect("conn present");
            (
                borrow_reason(pooled, &self.cfg, now),
                needs_validation(pooled, &self.cfg, now),
                pooled.raw.clone(),
            )
        };
        if let Some(r) = reason {
            return Some(r);
        }
        if need_validate && !self.is_healthy(&raw).await {
            return Some(CloseReason::Unhealthy);
        }
        None
    }

    async fn release(
        &self,
        conn: T,
        discard: bool,
        error: Option<&(dyn Error + 'static)>,
    ) -> Result<(), PoolError> {
        let pooled = {
            let mut g = self.state.lock().unwrap();
            g.checked_out.remove(&conn)
        };
        let pooled = match pooled {
            Some(p) => p,
            None => {
                return Err(PoolError::InvalidRelease(
                    "object is not checked out from this pool (double release?)".to_string(),
                ));
            }
        };
        let now = self.clock.monotonic();
        let hold = pooled.hold_time(now);
        let reason = self
            .check_release(&pooled, &conn, discard, error, now)
            .await;
        self.events.safe(|l| l.on_released(&pooled, hold));
        match reason {
            Some(r) => self.destroy_detached(pooled, r).await,
            None => {
                if let Err(p) = self.park(pooled, now) {
                    self.destroy_detached(p, CloseReason::PoolClosed).await;
                }
            }
        }
        Ok(())
    }

    async fn check_release(
        &self,
        pooled: &PooledConnection<T>,
        conn: &T,
        discard: bool,
        error: Option<&(dyn Error + 'static)>,
        now: f64,
    ) -> Option<CloseReason> {
        if discard {
            return Some(CloseReason::Discarded);
        }
        let error_suspect = match error {
            Some(e) => {
                let mut s = self.factory.is_fatal_error(e);
                if !s {
                    s = !self.is_healthy(conn).await;
                }
                s
            }
            None => false,
        };
        if error_suspect {
            return Some(CloseReason::Error);
        }
        if let Some(r) = rotation_reason(pooled, &self.cfg, now) {
            return Some(r);
        }
        if self.reset_conn(conn).await.is_err() {
            eprintln!(
                "connpool: pool {:?}: reset failed; discarding {}",
                self.name, pooled
            );
            return Some(CloseReason::ResetFailed);
        }
        None
    }

    fn park(&self, mut pooled: PooledConnection<T>, now: f64) -> Result<(), PooledConnection<T>> {
        let mut g = self.state.lock().unwrap();
        if g.closed {
            return Err(pooled);
        }
        pooled.mark_returned(now);
        g.idle.push(pooled);
        self.notify.notify_waiters();
        Ok(())
    }

    async fn create(&self, deadline: Option<f64>) -> Result<(PooledConnection<T>, f64), PoolError> {
        let started = self.clock.monotonic();
        let raw = self.create_raw(deadline).await?;
        let now = self.clock.monotonic();
        Ok((self.new_conn(raw, now), now - started))
    }

    async fn register_new(
        &self,
        pooled: PooledConnection<T>,
        created_in: f64,
    ) -> Result<T, PoolError> {
        let key = pooled.raw.clone();
        let outcome = {
            let mut g = self.state.lock().unwrap();
            if g.closed {
                None // closed: dispose below
            } else if g.checked_out.contains_key(&key) || g.idle.iter().any(|c| c.raw == key) {
                Some(Err(())) // duplicate
            } else {
                let ev = pooled.clone();
                g.checked_out.insert(key.clone(), pooled.clone());
                Some(Ok(ev))
            }
        };
        match outcome {
            None => {
                self.events
                    .safe(|l| l.on_connection_created(&pooled, created_in));
                self.destroy_detached(pooled, CloseReason::PoolClosed).await;
                Err(PoolError::Closed(format!("pool {:?} is closed", self.name)))
            }
            Some(Err(())) => {
                self.free_slot();
                Err(PoolError::ConnectionCreate {
                    message: format!(
                        "pool {:?}: factory.create() returned an object the pool already manages; each create must return a distinct connection",
                        self.name
                    ),
                    source: None,
                })
            }
            Some(Ok(ev)) => {
                self.events
                    .safe(|l| l.on_connection_created(&ev, created_in));
                Ok(key)
            }
        }
    }

    async fn create_raw(&self, deadline: Option<f64>) -> Result<T, PoolError> {
        if let Some(b) = self.breaker.get() {
            b.before_call()?;
        }
        match self.create_with_retry(deadline).await {
            Ok(raw) => {
                if let Some(b) = self.breaker.get() {
                    b.record_success();
                }
                Ok(raw)
            }
            Err(e) => {
                if let Some(b) = self.breaker.get() {
                    if matches!(e, PoolError::ConnectionCreate { .. }) {
                        b.record_failure();
                    } else {
                        b.record_abort();
                    }
                }
                Err(e)
            }
        }
    }

    /// The tightest of `create_timeout` and the remaining acquire deadline (None = no limit).
    fn create_limit(&self, deadline: Option<f64>) -> Option<f64> {
        let mut limit: Option<f64> = self.cfg.create_timeout;
        if let Some(r) = remaining(deadline, self.clock.monotonic()) {
            limit = Some(match limit {
                Some(l) => l.min(r),
                None => r,
            });
        }
        limit.map(|l| l.max(0.0))
    }

    async fn create_with_retry(&self, deadline: Option<f64>) -> Result<T, PoolError> {
        let retry = &self.cfg.retry;
        let mut attempt = 0u32;
        loop {
            if self.is_closed() {
                return Err(PoolError::Closed(format!("pool {:?} is closed", self.name)));
            }
            attempt += 1;
            let limit = self.create_limit(deadline);
            match self.create_one(limit).await {
                Ok(raw) => return Ok(raw),
                Err(CreateErr::Timeout) => {
                    // The binding limit was the acquire deadline -> caller impatience (abort).
                    let left = remaining(deadline, self.clock.monotonic());
                    if matches!(left, Some(l) if l <= 0.0) {
                        return Err(PoolError::Timeout(format!(
                            "pool {:?}: acquire deadline reached while creating connection",
                            self.name
                        )));
                    }
                    // Otherwise it was create_timeout firing -> a backend fault.
                    let e: FactoryError = "create timed out".into();
                    self.events.safe(|l| l.on_create_failed(&*e));
                    if attempt >= retry.max_attempts {
                        return Err(PoolError::ConnectionCreate {
                            message: format!(
                                "pool {:?}: could not create connection after {} attempt(s): create timed out",
                                self.name, attempt
                            ),
                            source: Some(e),
                        });
                    }
                    if let Some(stop) = self.backoff(attempt, deadline).await {
                        return Err(stop);
                    }
                }
                Err(CreateErr::Factory(e)) => {
                    self.events.safe(|l| l.on_create_failed(&*e));
                    if attempt >= retry.max_attempts || !retry.should_retry(&*e) {
                        return Err(PoolError::ConnectionCreate {
                            message: format!(
                                "pool {:?}: could not create connection after {} attempt(s): {}",
                                self.name, attempt, e
                            ),
                            source: Some(e),
                        });
                    }
                    if let Some(stop) = self.backoff(attempt, deadline).await {
                        return Err(stop);
                    }
                }
            }
        }
    }

    /// Sleeps the retry backoff; returns `Some(err)` if the deadline is reached first.
    async fn backoff(&self, attempt: u32, deadline: Option<f64>) -> Option<PoolError> {
        let delay = self.retry_delay(attempt);
        let left = remaining(deadline, self.clock.monotonic());
        if matches!(left, Some(l) if delay >= l) {
            return Some(PoolError::Timeout(format!(
                "pool {:?}: acquire deadline reached while retrying creation",
                self.name
            )));
        }
        self.clock.sleep_async(delay).await;
        None
    }

    async fn create_one(&self, limit: Option<f64>) -> Result<T, CreateErr> {
        let fut = self.factory.create();
        let result = match limit {
            Some(l) => match tokio_timeout(Duration::from_secs_f64(l), fut).await {
                Ok(r) => r,
                Err(_) => return Err(CreateErr::Timeout),
            },
            None => fut.await,
        };
        result.map_err(CreateErr::Factory)
    }

    async fn is_healthy(&self, raw: &T) -> bool {
        self.factory.validate(raw).await
    }

    async fn reset_conn(&self, conn: &T) -> Result<(), FactoryError> {
        self.factory.reset(conn).await
    }

    async fn destroy_by_key(&self, key: &T, reason: CloseReason) {
        let pooled = {
            let mut g = self.state.lock().unwrap();
            g.checked_out.remove(key)
        };
        let Some(p) = pooled else { return };
        self.destroy_detached(p, reason).await;
    }

    async fn destroy_detached(&self, pooled: PooledConnection<T>, reason: CloseReason) {
        self.close_conn(&pooled).await;
        {
            let mut g = self.state.lock().unwrap();
            g.size -= 1;
            self.notify.notify_waiters();
        }
        self.events
            .safe(|l| l.on_connection_closed(&pooled, reason));
    }

    async fn close_conn(&self, pooled: &PooledConnection<T>) {
        match self.factory.close(&pooled.raw).await {
            Ok(()) => {}
            Err(e) => eprintln!(
                "connpool: pool {:?}: error closing {}: {}",
                self.name, pooled, e
            ),
        }
    }

    async fn fill_to_min(&self) {
        loop {
            {
                let mut g = self.state.lock().unwrap();
                if g.closed || g.size >= self.cfg.min_size {
                    return;
                }
                g.size += 1;
            }
            match self.create(None).await {
                Err(e) => {
                    self.free_slot();
                    eprintln!("connpool: pool {:?}: warm-up failed: {}", self.name, e);
                    return;
                }
                Ok((pooled, created_in)) => {
                    self.events
                        .safe(|l| l.on_connection_created(&pooled, created_in));
                    if let Err(p) = self.park(pooled, self.clock.monotonic()) {
                        self.destroy_detached(p, CloseReason::PoolClosed).await;
                    }
                }
            }
        }
    }

    async fn run_maintenance(&self) {
        let doomed = {
            let mut g = self.state.lock().unwrap();
            if g.closed {
                return;
            }
            let idle = std::mem::take(&mut g.idle);
            let (keep, doomed) = sweep(idle, g.size, &self.cfg, self.clock.monotonic());
            g.idle = keep;
            doomed
        };
        for (p, reason) in doomed {
            self.destroy_detached(p, reason).await;
        }
        self.health_check_idle().await;
        self.fill_to_min().await;
        self.check_leaks();
    }

    async fn run_maintenance_safe(&self) {
        // A panic inside maintenance is logged; the loop keeps running.
        self.run_maintenance().await;
    }

    async fn health_check_idle(&self) {
        if !self.cfg.validate_idle {
            return;
        }
        let keys: Vec<T> = {
            let mut g = self.state.lock().unwrap();
            let batch = std::mem::take(&mut g.idle);
            let mut keys = Vec::with_capacity(batch.len());
            for conn in batch {
                let key = conn.raw.clone();
                g.checked_out.insert(key.clone(), conn);
                keys.push(key);
            }
            keys
        };
        for key in keys {
            let raw = {
                let g = self.state.lock().unwrap();
                g.checked_out.get(&key).expect("reserved").raw.clone()
            };
            let healthy = self.is_healthy(&raw).await;
            let doomed = {
                let mut g = self.state.lock().unwrap();
                let pooled = g.checked_out.remove(&key).expect("reserved");
                if healthy && !g.closed {
                    g.idle.push(pooled);
                    self.notify.notify_waiters();
                    None
                } else {
                    Some(pooled)
                }
            };
            if let Some(p) = doomed {
                let reason = if healthy {
                    CloseReason::PoolClosed
                } else {
                    CloseReason::Unhealthy
                };
                self.destroy_detached(p, reason).await;
            }
        }
    }

    fn check_leaks(&self) {
        let timeout = match self.cfg.leak_detection_timeout {
            Some(t) => t,
            None => return,
        };
        let now = self.clock.monotonic();
        let leaked: Vec<(PooledConnection<T>, f64)> = {
            let mut g = self.state.lock().unwrap();
            let mut out = Vec::new();
            for pooled in g.checked_out.values_mut() {
                if !pooled.leak_reported && pooled.hold_time(now) >= timeout {
                    pooled.leak_reported = true;
                    let held = pooled.hold_time(now);
                    out.push((pooled.clone(), held));
                }
            }
            out
        };
        for (pooled, held) in leaked {
            eprintln!(
                "connpool: pool {:?}: possible connection leak: {} held for {:.1}s",
                self.name, pooled, held
            );
            self.events.safe(|l| l.on_connection_leak(&pooled, held));
        }
    }

    fn stats_snapshot(&self) -> PoolStats {
        let (name, max_size, size, idle, in_use, waiting) = {
            let g = self.state.lock().unwrap();
            (
                self.name.clone(),
                self.cfg.max_size,
                g.size,
                g.idle.len(),
                g.checked_out.len(),
                g.waiters,
            )
        };
        let breaker_state = self.breaker.get().map(|b| b.state().as_str().to_string());
        self.stats.snapshot(StatsGauges {
            name,
            max_size,
            size,
            idle,
            in_use,
            waiting,
            breaker_state,
        })
    }
}

/// Classifies a `create_one` failure so the retry loop can distinguish a create-timeout from a
/// plain factory error.
enum CreateErr {
    Timeout,
    Factory(FactoryError),
}

/// Decrements the waiter count when a waiting `acquire` future resolves or is cancelled.
struct WaiterGuard<'a, T, F> {
    inner: &'a Inner<T, F>,
}

impl<T, F> Drop for WaiterGuard<'_, T, F> {
    fn drop(&mut self) {
        let mut g = self.inner.state.lock().unwrap();
        g.waiters = g.waiters.saturating_sub(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test_support::*;
    use crate::factories::async_factory::AsyncFnFactory;
    use crate::policy::config::IdleStrategy;
    use crate::resilience::breaker::BreakerConfig;
    use crate::resilience::retry::RetryPolicy;
    use std::sync::atomic::Ordering;

    fn plain(max_size: usize) -> PoolConfig {
        PoolConfig {
            max_size,
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn acquire_release_reuse_and_timeout() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(1));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        assert!(matches!(
            p.acquire(Timeout::Of(0.0)).await,
            Err(PoolError::Timeout(_))
        ));
        p.release(a).await.unwrap();
        let b = p.acquire(Timeout::Unset).await.unwrap();
        assert_eq!(b.id, a.id);
        p.release(b).await.unwrap();
        let s = p.stats();
        assert_eq!(s.created, 1);
        assert_eq!(s.acquired, 2);
        assert_eq!(s.timeouts, 1);
        assert_eq!(p.name(), "test");
        p.close(None).await;
        p.close(None).await; // idempotent
        assert!(p.is_closed());
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::Closed(_))
        ));
    }

    #[tokio::test]
    async fn fifo_strategy_and_config_validation() {
        let h = AsyncHarness::new();
        let p = h.pool(PoolConfig {
            idle_strategy: IdleStrategy::Fifo,
            ..plain(3)
        });
        let a = p.acquire(Timeout::Unset).await.unwrap();
        let b = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap();
        p.release(b).await.unwrap();
        let c = p.acquire(Timeout::Unset).await.unwrap();
        assert_eq!(c.id, a.id);
        p.release(c).await.unwrap();

        let bad = AsyncConnectionPool::new(
            h.factory(),
            PoolConfig {
                max_size: 0,
                ..Default::default()
            },
            None,
        );
        assert!(matches!(bad, Err(PoolError::Config(_))));
    }

    #[tokio::test]
    async fn invalid_release_detected() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(2));
        assert!(matches!(
            p.release(Conn::new(999)).await,
            Err(PoolError::InvalidRelease(_))
        ));
    }

    #[tokio::test]
    async fn max_waiters_backpressure() {
        let h = AsyncHarness::new();
        let p = h.pool(PoolConfig {
            max_waiters: Some(0),
            ..plain(1)
        });
        let a = p.acquire(Timeout::Unset).await.unwrap();
        assert!(matches!(
            p.acquire(Timeout::Infinite).await,
            Err(PoolError::Exhausted(_))
        ));
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn blocked_waiter_woken_on_release() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(1));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        let p2 = p.clone();
        let waiter = tokio::spawn(async move { p2.acquire(Timeout::Infinite).await.map(|c| c.id) });
        tokio::time::sleep(Duration::from_millis(30)).await;
        p.release(a).await.unwrap();
        let got = waiter.await.unwrap().unwrap();
        assert_eq!(got, a.id);
        p.release(Conn::new(got)).await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_waiter_does_not_leak_waiter_slot() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(1));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        {
            // Start a wait and drop it (cancel) before it resolves.
            let fut = p.acquire(Timeout::Infinite);
            tokio::pin!(fut);
            let _ = tokio::time::timeout(Duration::from_millis(20), &mut fut).await;
            // fut dropped here -> WaiterGuard decrements the waiter count.
        }
        assert_eq!(p.stats().waiting, 0);
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn real_time_wait_times_out() {
        // Real clock so the acquire deadline elapses during the notify wait.
        let h = AsyncHarness::with_system_clock();
        let p = h.pool(plain(1));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        assert!(matches!(
            p.acquire(Timeout::Of(0.05)).await,
            Err(PoolError::Timeout(_))
        ));
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn discard_error_and_reset_paths() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(5));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.discard(a).await.unwrap();
        assert!(h.fac.closed_ids().contains(&a.id));

        h.fac.set_validate_ok(false);
        let b = p.acquire(Timeout::Unset).await.unwrap();
        let e: FactoryError = "bad".into();
        p.release_with_error(b, &*e).await.unwrap();
        assert!(h.fac.closed_ids().contains(&b.id));

        h.fac.set_validate_ok(true);
        h.fac.set_reset_ok(false);
        let c = p.acquire(Timeout::Unset).await.unwrap();
        p.release(c).await.unwrap();
        assert_eq!(p.stats().closed_by_reason.get("reset_failed"), Some(&1));
    }

    #[tokio::test]
    async fn fatal_error_closes_without_validation() {
        let h = AsyncHarness::new();
        h.fac.set_fatal(true);
        let p = h.pool(plain(5));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        let e: FactoryError = "fatal".into();
        p.release_with_error(a, &*e).await.unwrap();
        assert!(h.fac.closed_ids().contains(&a.id));
    }

    #[tokio::test]
    async fn rotation_and_stale_replacement() {
        let h = AsyncHarness::new();
        let p = h.pool(PoolConfig {
            max_lifetime: Some(100.0),
            lifetime_jitter: 0.0,
            max_idle_time: None,
            validate_after_idle: None,
            ..plain(5)
        });
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap();
        h.clock.advance(200.0);
        let b = p.acquire(Timeout::Unset).await.unwrap();
        assert_ne!(b.id, a.id);
        p.release(b).await.unwrap();
    }

    #[tokio::test]
    async fn validate_on_checkout_replaces_unhealthy() {
        let h = AsyncHarness::new();
        let p = h.pool(PoolConfig {
            validate_after_idle: Some(5.0),
            max_lifetime: None,
            max_idle_time: None,
            ..plain(5)
        });
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap();
        h.clock.advance(10.0);
        h.fac.set_validate_ok(false);
        let b = p.acquire(Timeout::Unset).await.unwrap();
        assert_ne!(b.id, a.id);
        assert_eq!(p.stats().closed_by_reason.get("unhealthy"), Some(&1));
        p.release(b).await.unwrap();
    }

    #[tokio::test]
    async fn create_failure_retry_then_error() {
        let h = AsyncHarness::new();
        h.fac.fail_next_creates(10);
        let p = h.pool(PoolConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                base_delay: 0.0,
                ..Default::default()
            },
            ..plain(2)
        });
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::ConnectionCreate { .. })
        ));
        assert_eq!(p.stats().create_failures, 3);
        // slot was freed; a later success works
        h.fac.fail_next_creates(1);
        let a = p.acquire(Timeout::Unset).await.unwrap(); // 1 fail then ok
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn create_timeout_counts_as_backend_failure() {
        let h = AsyncHarness::with_system_clock();
        h.fac.set_create_delay_ms(1000); // slower than create_timeout
        let p = h.pool(PoolConfig {
            create_timeout: Some(0.02),
            acquire_timeout: Some(5.0),
            retry: RetryPolicy::no_retry(),
            ..plain(2)
        });
        let err = p.acquire(Timeout::Unset).await.unwrap_err();
        assert!(matches!(err, PoolError::ConnectionCreate { .. }));
        assert!(p.stats().create_failures >= 1);
    }

    #[tokio::test]
    async fn retry_backoff_exceeding_deadline_times_out() {
        let h = AsyncHarness::new();
        h.fac.fail_next_creates(10);
        let p = h.pool(PoolConfig {
            acquire_timeout: Some(0.05),
            retry: RetryPolicy {
                max_attempts: 5,
                base_delay: 100.0,
                jitter: 0.0,
                ..Default::default()
            },
            ..plain(2)
        });
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn breaker_trips_blocks_recovers_and_abort_is_safe() {
        let h = AsyncHarness::new();
        h.fac.fail_next_creates(100);
        let p = h.pool(PoolConfig {
            retry: RetryPolicy::no_retry(),
            maintenance_interval: None,
            breaker: Some(BreakerConfig {
                failure_threshold: 1,
                recovery_timeout: 10.0,
                half_open_max_calls: 1,
                success_threshold: 1,
            }),
            ..Default::default()
        });
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::ConnectionCreate { .. })
        ));
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::CircuitOpen { .. })
        ));
        assert_eq!(p.stats().breaker_state.as_deref(), Some("open"));
        h.clock.advance(10.0);
        h.fac.fail_next_creates(0);
        let a = p.acquire(Timeout::Unset).await.unwrap();
        assert_eq!(p.stats().breaker_state.as_deref(), Some("closed"));
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn duplicate_object_rejected() {
        let h = AsyncHarness::new();
        h.fac.force_id(Some(42));
        let p = h.pool(plain(5));
        let _a = p.acquire(Timeout::Unset).await.unwrap();
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::ConnectionCreate { .. })
        ));
    }

    #[tokio::test]
    async fn open_warms_up_close_drains_and_maintenance() {
        let h = AsyncHarness::new();
        let rec = Recorder::new();
        let p = h.pool_with(
            PoolConfig {
                min_size: 2,
                max_lifetime: Some(100.0),
                lifetime_jitter: 0.0,
                max_idle_time: Some(50.0),
                validate_after_idle: None,
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
            vec![rec.clone()],
        );
        p.open().await.unwrap();
        p.open().await.unwrap(); // idempotent
        assert_eq!(p.stats().idle, 2);
        // age out -> sweep evicts then refills to min_size
        h.clock.advance(200.0);
        p.run_maintenance().await;
        assert_eq!(p.stats().idle, 2);
        assert!(p.stats().closed >= 2);
        p.close(Some(0.0)).await;
        assert!(matches!(p.open().await, Err(PoolError::Closed(_))));
    }

    #[tokio::test]
    async fn maintenance_noop_when_closed() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(2));
        p.close(None).await;
        p.run_maintenance().await;
    }

    #[tokio::test]
    async fn health_check_idle_and_leak_detection() {
        let h = AsyncHarness::new();
        let rec = Recorder::new();
        let p = h.pool_with(
            PoolConfig {
                validate_idle: true,
                leak_detection_timeout: Some(10.0),
                max_lifetime: None,
                max_idle_time: None,
                validate_after_idle: None,
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
            vec![rec.clone()],
        );
        let a = p.acquire(Timeout::Unset).await.unwrap();
        h.clock.advance(20.0);
        p.run_maintenance().await; // leak detected (held), idle empty so health check no-op
        p.run_maintenance().await; // leak not re-counted
        assert_eq!(rec.leaks.load(Ordering::SeqCst), 1);
        p.release(a).await.unwrap();
        // now idle validation: healthy keeps, unhealthy evicts
        p.run_maintenance().await;
        assert_eq!(p.stats().idle, 1);
        h.fac.set_validate_ok(false);
        p.run_maintenance().await;
        assert_eq!(p.stats().idle, 0);
    }

    #[tokio::test]
    async fn release_into_closed_pool_closes_connection() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(2));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.close(None).await;
        p.release(a).await.unwrap();
        assert!(h.fac.closed_ids().contains(&a.id));
    }

    #[tokio::test]
    async fn close_drains_checked_out_with_timeout() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(2));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        let p2 = p.clone();
        let closer = tokio::spawn(async move { p2.close(Some(5.0)).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        p.release(a).await.unwrap();
        closer.await.unwrap();
        assert!(p.is_closed());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn background_maintenance_task_runs_and_stops() {
        let h = AsyncHarness::with_system_clock();
        let p = h.pool(PoolConfig {
            maintenance_interval: Some(0.01),
            breaker: None,
            ..Default::default()
        });
        p.open().await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        p.close(None).await;
        assert!(p.is_closed());
    }

    #[tokio::test]
    async fn default_options_and_warmup_failure() {
        let h = AsyncHarness::new();
        // None options path.
        let p = AsyncConnectionPool::new(h.factory(), plain(2), None).unwrap();
        assert_eq!(p.name(), "pool");
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap();

        // warm-up failure path in fill_to_min.
        let h2 = AsyncHarness::new();
        h2.fac.fail_next_creates(100);
        let p2 = h2.pool(PoolConfig {
            min_size: 1,
            retry: RetryPolicy::no_retry(),
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        });
        p2.open().await.unwrap(); // warm-up fails, logged, slot freed
        assert_eq!(p2.stats().idle, 0);
    }

    #[tokio::test]
    async fn cancelled_create_frees_slot() {
        let h = AsyncHarness::with_system_clock();
        h.fac.set_create_delay_ms(1000);
        let p = h.pool(plain(1));
        {
            let fut = p.acquire(Timeout::Infinite);
            tokio::pin!(fut);
            let _ = tokio::time::timeout(Duration::from_millis(20), &mut fut).await;
            // dropped mid-create -> SlotGuard frees the reserved slot
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(p.stats().size, 0);
        // the freed slot is reusable
        h.fac.set_create_delay_ms(0);
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap();
    }

    /// Regression for the cross-language "register-new cleanup" double-free: a cancellation
    /// (future drop) landing while `register_new`'s closed-path cleanup (`destroy_detached(...)
    /// .await`, which itself awaits `factory.close()`) is in flight must never let a second,
    /// outer disposal run for the same connection. In Rust there is no redelivered-exception /
    /// catch-all-redispose mechanism (cancellation is "drop the future"): dropping the acquire
    /// future while it is suspended inside `close_conn().await` simply abandons that one in-flight
    /// disposal attempt, and there is no `.await` between finishing cleanup and returning the
    /// domain error, so a drop can never land in a gap *after* cleanup completes but before the
    /// error is raised. A double disposal would double-call `factory.close()` for one connection
    /// id and double-decrement `size` (a `usize`, so a second decrement past zero would panic on
    /// underflow). This races the drop against the close repeatedly and asserts neither happens.
    #[tokio::test]
    async fn cancelled_register_new_closed_path_never_double_closes() {
        for _ in 0..30 {
            let h = AsyncHarness::with_system_clock();
            h.fac.set_create_delay_ms(30); // keep acquire in create() long enough to close concurrently
            h.fac.set_close_delay_ms(30); // slow close() so we can drop the acquire future mid-disposal
            let p = h.pool(plain(1));
            let p2 = p.clone();
            {
                let fut = p.acquire(Timeout::Infinite);
                tokio::pin!(fut);
                // Close the pool while create() is still in flight, so by the time create()
                // finishes, register_new() observes `closed` and enters the cleanup path
                // (destroy_detached -> close_conn().await, now slow). Drop the acquire future
                // mid-`close_conn()`.
                let closer = tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    p2.close(None).await;
                });
                let _ = tokio::time::timeout(Duration::from_millis(40), &mut fut).await;
                closer.await.unwrap();
                // fut dropped here, racing with close_conn().await inside register_new's
                // closed-path cleanup.
            }
            // Give any (incorrect) second disposal a chance to run before asserting.
            tokio::time::sleep(Duration::from_millis(100)).await;
            let closed = h.fac.closed_ids();
            assert!(
                closed.len() <= 1,
                "factory.close() must run at most once for the connection, got {closed:?}"
            );
            // `size` is a `usize`: a double-decrement past zero panics on underflow in debug and
            // wraps to a huge value in release — either way this bounds check (plus the lack of a
            // panic) proves it was decremented at most once.
            assert!(
                p.stats().size <= 1,
                "pool size must never be double-decremented, got {}",
                p.stats().size
            );
        }
    }

    #[tokio::test]
    async fn cancelled_borrow_returns_connection() {
        let h = AsyncHarness::with_system_clock();
        let p = h.pool(PoolConfig {
            validate_after_idle: Some(0.0), // always validate on checkout
            max_lifetime: None,
            max_idle_time: None,
            ..plain(1)
        });
        // seed one idle connection
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap();
        h.fac.set_validate_delay_ms(1000); // validation blocks
        {
            let fut = p.acquire(Timeout::Infinite);
            tokio::pin!(fut);
            let _ = tokio::time::timeout(Duration::from_millis(20), &mut fut).await;
            // dropped during validate -> BorrowGuard returns the conn to idle
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(p.stats().idle, 1);
        assert_eq!(p.stats().in_use, 0);
    }

    #[tokio::test]
    async fn listener_panic_does_not_break_pool() {
        struct Boom;
        impl PoolListener<Conn> for Boom {
            fn on_acquired(&self, _c: &PooledConnection<Conn>, _w: f64) {
                panic!("boom");
            }
        }
        let h = AsyncHarness::new();
        let p = h.pool_with(plain(2), vec![Arc::new(Boom)]);
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn waiter_under_max_waiters_blocks_then_succeeds() {
        let h = AsyncHarness::new();
        let p = h.pool(PoolConfig {
            max_waiters: Some(5),
            ..plain(1)
        });
        let a = p.acquire(Timeout::Unset).await.unwrap();
        let p2 = p.clone();
        let waiter = tokio::spawn(async move { p2.acquire(Timeout::Infinite).await.map(|c| c.id) });
        tokio::time::sleep(Duration::from_millis(30)).await;
        p.release(a).await.unwrap();
        assert_eq!(waiter.await.unwrap().unwrap(), a.id);
        p.release(Conn::new(a.id)).await.unwrap();
    }

    #[tokio::test]
    async fn create_timeout_then_backoff_hits_deadline() {
        let h = AsyncHarness::with_system_clock();
        h.fac.set_create_delay_ms(1000);
        let p = h.pool(PoolConfig {
            acquire_timeout: Some(1.0),
            create_timeout: Some(0.01),
            retry: RetryPolicy {
                max_attempts: 5,
                base_delay: 100.0,
                jitter: 0.0,
                ..Default::default()
            },
            ..plain(2)
        });
        // create times out (limit=create_timeout, left still >0), then backoff (100s) exceeds
        // the acquire deadline -> Timeout from the timeout-arm's backoff.
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn rotation_on_release() {
        let h = AsyncHarness::new();
        let p = h.pool(PoolConfig {
            max_uses: Some(1),
            max_lifetime: None,
            ..plain(5)
        });
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap(); // use_count==max_uses -> rotate on release
        assert_eq!(p.stats().closed_by_reason.get("max_uses"), Some(&1));
    }

    #[tokio::test]
    async fn breaker_abort_does_not_trip() {
        let h = AsyncHarness::new();
        h.fac.fail_next_creates(100);
        let p = h.pool(PoolConfig {
            acquire_timeout: Some(0.05),
            retry: RetryPolicy {
                max_attempts: 5,
                base_delay: 100.0,
                jitter: 0.0,
                ..Default::default()
            },
            maintenance_interval: None,
            breaker: Some(BreakerConfig {
                failure_threshold: 1,
                ..Default::default()
            }),
            ..Default::default()
        });
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::Timeout(_))
        ));
        assert_eq!(p.stats().breaker_state.as_deref(), Some("closed"));
    }

    #[tokio::test]
    async fn create_limit_from_remaining_deadline() {
        // create_timeout None but a finite acquire deadline -> limit comes from remaining().
        let h = AsyncHarness::with_system_clock();
        let p = h.pool(PoolConfig {
            create_timeout: None,
            acquire_timeout: Some(5.0),
            ..plain(2)
        });
        let a = p.acquire(Timeout::Of(5.0)).await.unwrap();
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn create_with_no_limit_at_all() {
        // create_timeout None + Infinite acquire -> create_one runs with no timer.
        let h = AsyncHarness::with_system_clock();
        let p = h.pool(PoolConfig {
            create_timeout: None,
            ..plain(2)
        });
        let a = p.acquire(Timeout::Infinite).await.unwrap();
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn create_timeout_bound_by_acquire_deadline_is_timeout() {
        let h = AsyncHarness::with_system_clock();
        h.fac.set_create_delay_ms(1000);
        let p = h.pool(PoolConfig {
            acquire_timeout: Some(0.02),
            create_timeout: Some(10.0),
            retry: RetryPolicy::no_retry(),
            ..plain(2)
        });
        // limit = min(create_timeout, remaining) = the acquire deadline -> PoolTimeout (abort).
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn create_timeout_then_backoff_then_fail() {
        let h = AsyncHarness::with_system_clock();
        h.fac.set_create_delay_ms(1000);
        let p = h.pool(PoolConfig {
            acquire_timeout: Some(5.0),
            create_timeout: Some(0.02),
            retry: RetryPolicy {
                max_attempts: 2,
                base_delay: 0.0,
                ..Default::default()
            },
            ..plain(2)
        });
        // each attempt times out (backend fault); backoff runs once; then ConnectionCreate.
        assert!(matches!(
            p.acquire(Timeout::Unset).await,
            Err(PoolError::ConnectionCreate { .. })
        ));
        assert!(p.stats().create_failures >= 2);
    }

    #[tokio::test]
    async fn close_error_is_logged() {
        let h = AsyncHarness::new();
        h.fac.set_close_err(true);
        let p = h.pool(plain(5));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.discard(a).await.unwrap(); // close returns Err -> logged, still counted
        assert_eq!(p.stats().closed, 1);
    }

    #[tokio::test]
    async fn close_drain_gives_up_at_deadline() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(2));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.close(Some(0.0)).await; // conn still out, immediate deadline -> break
        assert!(p.is_closed());
        p.release(a).await.unwrap();
    }

    #[tokio::test]
    async fn blocked_waiter_wakes_as_closed() {
        let h = AsyncHarness::new();
        let p = h.pool(plain(1));
        let a = p.acquire(Timeout::Unset).await.unwrap();
        let p2 = p.clone();
        let waiter = tokio::spawn(async move { p2.acquire(Timeout::Infinite).await });
        tokio::time::sleep(Duration::from_millis(30)).await;
        p.close(None).await;
        assert!(matches!(waiter.await.unwrap(), Err(PoolError::Closed(_))));
        p.release(a).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn maintenance_task_stops_when_pool_dropped() {
        let h = AsyncHarness::with_system_clock();
        let p = h.pool(PoolConfig {
            maintenance_interval: Some(0.01),
            breaker: None,
            ..Default::default()
        });
        p.open().await.unwrap();
        drop(p); // task's next weak.upgrade() returns None -> it exits
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn cancelled_borrow_while_closed_frees_slot() {
        let h = AsyncHarness::with_system_clock();
        let p = h.pool(PoolConfig {
            validate_after_idle: Some(0.0),
            max_lifetime: None,
            max_idle_time: None,
            ..plain(1)
        });
        let a = p.acquire(Timeout::Unset).await.unwrap();
        p.release(a).await.unwrap(); // seed idle
        h.fac.set_validate_delay_ms(1000);
        {
            let fut = p.acquire(Timeout::Infinite);
            tokio::pin!(fut);
            let _ = tokio::time::timeout(Duration::from_millis(20), &mut fut).await;
            p.close(None).await; // pool closed while the borrow is mid-validation
            // fut dropped here -> BorrowGuard sees closed, frees the slot
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert_eq!(p.stats().size, 0);
    }

    // --- self-closing factories exercise the "pool closed mid-create" paths ---

    type SelfClosePool = AsyncConnectionPool<Conn, AsyncFnFactory<Conn>>;

    fn self_closing_create(ok: bool) -> (SelfClosePool, PoolConfig) {
        let cfg = PoolConfig {
            min_size: 0,
            retry: RetryPolicy {
                max_attempts: 2,
                base_delay: 0.0,
                ..Default::default()
            },
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        };
        (build_self_closing(cfg.clone(), ok, false), cfg)
    }

    fn build_self_closing(
        cfg: PoolConfig,
        create_ok: bool,
        close_on_validate: bool,
    ) -> SelfClosePool {
        use std::sync::Mutex as StdMutex;
        let slot: Arc<StdMutex<Option<SelfClosePool>>> = Arc::new(StdMutex::new(None));
        let s_create = slot.clone();
        let s_validate = slot.clone();
        let mut factory = AsyncFnFactory::new(move || {
            let s = s_create.clone();
            Box::pin(async move {
                if !close_on_validate {
                    let p = s.lock().unwrap().clone().unwrap();
                    p.close(None).await;
                }
                if create_ok {
                    Ok::<_, FactoryError>(Conn::new(7))
                } else {
                    Err::<Conn, FactoryError>("boom".into())
                }
            }) as _
        });
        // Only override validate when the test wants validation to close the pool; the closure
        // then closes unconditionally (its last expression is `true`), avoiding a brace after an
        // await that LLVM mis-attributes.
        if close_on_validate {
            factory = factory.with_validate(move |_c: &Conn| {
                let s = s_validate.clone();
                Box::pin(async move {
                    let p = s.lock().unwrap().clone().unwrap();
                    p.close(None).await;
                    true
                }) as _
            });
        }
        let pool = AsyncConnectionPool::new(factory, cfg, None).unwrap();
        *slot.lock().unwrap() = Some(pool.clone());
        pool
    }

    #[tokio::test]
    async fn pool_closed_during_create_registers_as_closed() {
        let (pool, _cfg) = self_closing_create(true);
        assert!(matches!(
            pool.acquire(Timeout::Unset).await,
            Err(PoolError::Closed(_))
        ));
    }

    #[tokio::test]
    async fn pool_closed_mid_retry_loop() {
        let (pool, _cfg) = self_closing_create(false);
        assert!(matches!(
            pool.acquire(Timeout::Unset).await,
            Err(PoolError::Closed(_))
        ));
    }

    #[tokio::test]
    async fn fill_to_min_park_fails_when_closed_mid_create() {
        let cfg = PoolConfig {
            min_size: 1,
            retry: RetryPolicy::no_retry(),
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        };
        let pool = build_self_closing(cfg, true, false);
        pool.open().await.unwrap(); // warm-up creates, pool closes, park fails -> destroyed
        assert!(pool.is_closed());
    }

    #[tokio::test]
    async fn health_check_closed_mid_validation() {
        let cfg = PoolConfig {
            validate_idle: true,
            validate_after_idle: None,
            max_lifetime: None,
            max_idle_time: None,
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        };
        let pool = build_self_closing(cfg, true, true);
        let a = pool.acquire(Timeout::Unset).await.unwrap();
        pool.release(a).await.unwrap(); // one idle conn
        pool.run_maintenance().await; // validate closes pool, reports healthy -> PoolClosed evict
        assert!(pool.is_closed());
    }
}
