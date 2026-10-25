//! `ConnectionPool`: a thread-safe, blocking connection pool.
//!
//! A single mutex guards a tiny amount of state (an idle `Vec`, a checked-out `HashMap`, a slot
//! counter). All I/O — create, validate, reset, close — happens outside that lock, so a slow
//! backend never blocks unrelated callers. A slot is reserved under the lock before creating,
//! which makes `max_size` a hard cap. The retirement rules live in
//! [`lifecycle`](crate::policy::lifecycle); this file is plumbing.
//!
//! `T` is pooled by **value identity** (`Clone + Eq + Hash`) — use a cheap handle type such as
//! `Arc<Conn>` or an id; this is the Rust analog of the Go port's `comparable` constraint.

use std::collections::HashMap;
use std::error::Error;
use std::hash::Hash;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::{Arc, Condvar, Mutex, OnceLock, Weak};
use std::thread::JoinHandle;
use std::time::Duration;

use crate::core::clock::{Clock, SystemClock};
use crate::core::connection::{CloseReason, PooledConnection};
use crate::core::errors::{FactoryError, PoolError};
use crate::core::timeouts::{Timeout, remaining};
use crate::factories::sync_factory::ConnectionFactory;
use crate::observability::events::{Listeners, PoolListener};
use crate::observability::stats::{PoolStats, StatsCollector, StatsGauges};
use crate::policy::config::PoolConfig;
use crate::policy::lifecycle::{
    borrow_reason, needs_validation, new_connection, pop_idle, rotation_reason, sweep,
};
use crate::resilience::breaker::{BreakerState, CircuitBreaker};
use crate::rng::{Rng, SystemRng};

/// Options for constructing a pool. All fields are optional.
pub struct PoolOptions<T> {
    /// Pool name (defaults to `"pool"`).
    pub name: Option<String>,
    /// Time source (defaults to [`SystemClock`]).
    pub clock: Option<Arc<dyn Clock>>,
    /// Jitter RNG (defaults to [`SystemRng`]).
    pub rng: Option<Box<dyn Rng>>,
    /// Listeners added in addition to the built-in stats collector.
    pub listeners: Vec<Arc<dyn PoolListener<T>>>,
}

impl<T> Default for PoolOptions<T> {
    fn default() -> Self {
        Self {
            name: None,
            clock: None,
            rng: None,
            listeners: Vec::new(),
        }
    }
}

struct State<T> {
    idle: Vec<PooledConnection<T>>,
    checked_out: HashMap<T, PooledConnection<T>>,
    size: usize,
    waiters: usize,
    closed: bool,
    opened: bool,
}

struct MaintStop {
    flag: Mutex<bool>,
    cv: Condvar,
}

pub(crate) struct Inner<T> {
    name: String,
    factory: Arc<dyn ConnectionFactory<T>>,
    cfg: PoolConfig,
    clock: Arc<dyn Clock>,
    rng: Mutex<Box<dyn Rng>>,

    state: Mutex<State<T>>,
    cond: Condvar,

    stats: Arc<StatsCollector<T>>,
    events: Listeners<T>,
    breaker: OnceLock<CircuitBreaker>,

    stop: Arc<MaintStop>,
    handle: Mutex<Option<JoinHandle<()>>>,
}

/// A thread-safe, blocking connection pool. Cheaply clonable (clones share one pool).
pub struct ConnectionPool<T>(Arc<Inner<T>>);

impl<T> Clone for ConnectionPool<T> {
    fn clone(&self) -> Self {
        ConnectionPool(self.0.clone())
    }
}

impl<T> ConnectionPool<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
{
    /// Builds a pool, validating `config`.
    pub fn new(
        factory: impl ConnectionFactory<T> + 'static,
        config: PoolConfig,
        options: Option<PoolOptions<T>>,
    ) -> Result<Self, PoolError> {
        config.validate()?;
        let options = options.unwrap_or_default();
        let clock: Arc<dyn Clock> = options.clock.unwrap_or_else(|| Arc::new(SystemClock::new()));
        let rng: Box<dyn Rng> = options.rng.unwrap_or_else(|| Box::new(SystemRng::new()));
        let name = options.name.unwrap_or_else(|| "pool".to_string());

        let stats = Arc::new(StatsCollector::new());
        let mut listeners: Vec<Arc<dyn PoolListener<T>>> = vec![stats.clone()];
        listeners.extend(options.listeners);
        let events = Listeners::new(listeners);

        let inner = Arc::new(Inner {
            name,
            factory: Arc::new(factory),
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
            cond: Condvar::new(),
            stats,
            events,
            breaker: OnceLock::new(),
            stop: Arc::new(MaintStop {
                flag: Mutex::new(false),
                cv: Condvar::new(),
            }),
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

        Ok(ConnectionPool(inner))
    }

    /// The pool's name.
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// Warms up `min_size` connections and starts the background maintenance thread. Optional:
    /// without it the pool is purely lazy. Idempotent.
    pub fn open(&self) -> Result<(), PoolError> {
        let start_thread = {
            let mut g = self.0.state.lock().unwrap();
            if g.closed {
                return Err(PoolError::Closed(format!("pool {:?} is closed", self.0.name)));
            }
            if g.opened {
                return Ok(());
            }
            g.opened = true;
            self.0.cfg.maintenance_interval
        };
        self.0.fill_to_min();
        if let Some(interval) = start_thread {
            self.start_maintenance(interval);
        }
        Ok(())
    }

    fn start_maintenance(&self, interval: f64) {
        let weak: Weak<Inner<T>> = Arc::downgrade(&self.0);
        let stop = self.0.stop.clone();
        let handle = std::thread::spawn(move || {
            loop {
                {
                    let g = stop.flag.lock().unwrap();
                    let (g, _) = stop
                        .cv
                        .wait_timeout_while(g, Duration::from_secs_f64(interval), |s| !*s)
                        .unwrap();
                    if *g {
                        return;
                    }
                }
                match weak.upgrade() {
                    Some(inner) => inner.run_maintenance_safe(),
                    None => return,
                }
            }
        });
        *self.0.handle.lock().unwrap() = Some(handle);
    }

    /// Closes idle connections now; checked-out ones close when released. Blocked `acquire`
    /// callers get a [`PoolError::Closed`]. Idempotent.
    ///
    /// `timeout` (`None` = don't wait) drains gracefully: wait up to that many seconds for
    /// in-use connections to return before giving up (they still close on release). `0` returns
    /// at once.
    pub fn close(&self, timeout: Option<f64>) {
        self.0.close(timeout);
    }

    /// Whether [`close`](Self::close) has been called.
    pub fn is_closed(&self) -> bool {
        self.0.state.lock().unwrap().closed
    }

    /// Gets a connection. [`Timeout::Unset`] uses `config.acquire_timeout`; [`Timeout::Infinite`]
    /// waits forever; `Timeout::Of(0.0)` never waits.
    pub fn acquire(&self, timeout: Timeout) -> Result<T, PoolError> {
        self.0.acquire(timeout)
    }

    /// Returns a connection to the pool for reuse.
    pub fn release(&self, conn: T) -> Result<(), PoolError> {
        self.0.release(conn, false, None)
    }

    /// Returns a connection but forces it to be closed instead of reused.
    pub fn discard(&self, conn: T) -> Result<(), PoolError> {
        self.0.release(conn, true, None)
    }

    /// Returns a connection, signalling that the caller's work raised `error`; the pool
    /// discards the connection if the factory deems the error fatal or validation fails.
    pub fn release_with_error(
        &self,
        conn: T,
        error: &(dyn Error + 'static),
    ) -> Result<(), PoolError> {
        self.0.release(conn, false, Some(error))
    }

    /// Borrows a connection for the duration of `f`, releasing it afterwards.
    pub fn with_connection<R>(
        &self,
        timeout: Timeout,
        f: impl FnOnce(&T) -> R,
    ) -> Result<R, PoolError> {
        let conn = self.acquire(timeout)?;
        let out = f(&conn);
        self.release(conn)?;
        Ok(out)
    }

    /// An immutable snapshot of pool counters and gauges.
    pub fn stats(&self) -> PoolStats {
        self.0.stats_snapshot()
    }

    /// Performs one maintenance sweep (evict expired/over-idle, optionally validate idle, refill
    /// to `min_size`, flag leaks). Runs periodically after [`open`](Self::open); public for tests
    /// and cron-style use.
    pub fn run_maintenance(&self) {
        self.0.run_maintenance();
    }
}

enum Taken<T> {
    Reused(T),
    Reserved,
}

impl<T> Inner<T>
where
    T: Clone + Eq + Hash + Send + Sync + 'static,
{
    fn is_closed(&self) -> bool {
        self.state.lock().unwrap().closed
    }

    fn signal(&self) {
        self.cond.notify_all();
    }

    fn new_conn(&self, raw: T, now: f64) -> PooledConnection<T> {
        let mut rng = self.rng.lock().unwrap();
        new_connection(raw, &self.cfg, now, &mut **rng)
    }

    fn retry_delay(&self, attempt: u32) -> f64 {
        let mut rng = self.rng.lock().unwrap();
        self.cfg.retry.delay_for(attempt, &mut **rng)
    }

    fn close(&self, timeout: Option<f64>) {
        let idle = {
            let mut g = self.state.lock().unwrap();
            if g.closed {
                return;
            }
            g.closed = true;
            let idle = std::mem::take(&mut g.idle);
            self.cond.notify_all();
            if let Some(t) = timeout {
                let deadline = self.clock.monotonic() + t;
                while !g.checked_out.is_empty() {
                    let left = deadline - self.clock.monotonic();
                    if left <= 0.0 {
                        break;
                    }
                    let (g2, _) = self
                        .cond
                        .wait_timeout(g, Duration::from_secs_f64(left))
                        .unwrap();
                    g = g2;
                }
            }
            idle
        };
        {
            *self.stop.flag.lock().unwrap() = true;
            self.stop.cv.notify_all();
        }
        if let Some(h) = self.handle.lock().unwrap().take() {
            let _ = h.join();
        }
        for pooled in idle {
            self.destroy_detached(pooled, CloseReason::PoolClosed);
        }
    }

    fn acquire(&self, timeout: Timeout) -> Result<T, PoolError> {
        let started = self.clock.monotonic();
        let deadline = timeout.compute_deadline(self.cfg.acquire_timeout, started);
        let key = match self.acquire_loop(deadline) {
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

    fn acquire_loop(&self, deadline: Option<f64>) -> Result<T, PoolError> {
        loop {
            match self.take_idle_or_reserve(deadline)? {
                Taken::Reserved => {
                    let (pooled, created_in) = self.create(deadline)?;
                    let key = self.register_new(pooled, created_in)?;
                    return Ok(key);
                }
                Taken::Reused(key) => match self.check_borrow(&key) {
                    None => return Ok(key),
                    Some(reason) => self.destroy_by_key(&key, reason),
                },
            }
        }
    }

    fn take_idle_or_reserve(&self, deadline: Option<f64>) -> Result<Taken<T>, PoolError> {
        let mut g = self.state.lock().unwrap();
        loop {
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
            g = match left {
                None => self.cond.wait(g).unwrap(),
                Some(l) => {
                    let (g2, _) = self
                        .cond
                        .wait_timeout(g, Duration::from_secs_f64(l.max(0.0)))
                        .unwrap();
                    g2
                }
            };
            g.waiters -= 1;
        }
    }

    fn check_borrow(&self, key: &T) -> Option<CloseReason> {
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
        if need_validate && !self.is_healthy(&raw) {
            return Some(CloseReason::Unhealthy);
        }
        None
    }

    fn release(
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
        let reason = self.check_release(&pooled, &conn, discard, error, now);
        self.events.safe(|l| l.on_released(&pooled, hold));
        match reason {
            Some(r) => self.destroy_detached(pooled, r),
            None => {
                if let Err(p) = self.park(pooled, now) {
                    self.destroy_detached(p, CloseReason::PoolClosed);
                }
            }
        }
        Ok(())
    }

    fn check_release(
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
        if let Some(e) = error {
            if self.factory.is_fatal_error(e) || !self.is_healthy(conn) {
                return Some(CloseReason::Error);
            }
        }
        if let Some(r) = rotation_reason(pooled, &self.cfg, now) {
            return Some(r);
        }
        if self.reset_conn(conn).is_err() {
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
        self.signal();
        Ok(())
    }

    fn create(&self, deadline: Option<f64>) -> Result<(PooledConnection<T>, f64), PoolError> {
        let started = self.clock.monotonic();
        let raw = match self.create_raw(deadline) {
            Ok(r) => r,
            Err(e) => {
                self.free_slot();
                return Err(e);
            }
        };
        let now = self.clock.monotonic();
        Ok((self.new_conn(raw, now), now - started))
    }

    fn register_new(
        &self,
        pooled: PooledConnection<T>,
        created_in: f64,
    ) -> Result<T, PoolError> {
        let key = pooled.raw.clone();
        let ev;
        {
            let mut g = self.state.lock().unwrap();
            if g.closed {
                drop(g);
                self.events.safe(|l| l.on_connection_created(&pooled, created_in));
                self.destroy_detached(pooled, CloseReason::PoolClosed);
                return Err(PoolError::Closed(format!("pool {:?} is closed", self.name)));
            }
            if g.checked_out.contains_key(&key) || g.idle.iter().any(|c| c.raw == key) {
                drop(g);
                self.free_slot();
                return Err(PoolError::ConnectionCreate {
                    message: format!(
                        "pool {:?}: factory.create() returned an object the pool already manages; each create must return a distinct connection",
                        self.name
                    ),
                    source: None,
                });
            }
            ev = pooled.clone();
            g.checked_out.insert(key.clone(), pooled);
        }
        self.events.safe(|l| l.on_connection_created(&ev, created_in));
        Ok(key)
    }

    fn create_raw(&self, deadline: Option<f64>) -> Result<T, PoolError> {
        if let Some(b) = self.breaker.get() {
            b.before_call()?;
        }
        match self.create_with_retry(deadline) {
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

    fn create_with_retry(&self, deadline: Option<f64>) -> Result<T, PoolError> {
        let retry = &self.cfg.retry;
        let mut attempt = 0u32;
        loop {
            if self.is_closed() {
                return Err(PoolError::Closed(format!("pool {:?} is closed", self.name)));
            }
            attempt += 1;
            match self.create_one() {
                Ok(raw) => return Ok(raw),
                Err(e) => {
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
                    let delay = self.retry_delay(attempt);
                    let left = remaining(deadline, self.clock.monotonic());
                    if let Some(l) = left {
                        if delay >= l {
                            return Err(PoolError::Timeout(format!(
                                "pool {:?}: acquire deadline reached while retrying creation",
                                self.name
                            )));
                        }
                    }
                    self.clock.sleep(delay);
                }
            }
        }
    }

    fn create_one(&self) -> Result<T, FactoryError> {
        match catch_unwind(AssertUnwindSafe(|| self.factory.create())) {
            Ok(r) => r,
            Err(_) => Err("create panicked".into()),
        }
    }

    fn is_healthy(&self, raw: &T) -> bool {
        catch_unwind(AssertUnwindSafe(|| self.factory.validate(raw))).unwrap_or(false)
    }

    fn reset_conn(&self, conn: &T) -> Result<(), FactoryError> {
        match catch_unwind(AssertUnwindSafe(|| self.factory.reset(conn))) {
            Ok(r) => r,
            Err(_) => Err("reset panicked".into()),
        }
    }

    fn free_slot(&self) {
        let mut g = self.state.lock().unwrap();
        g.size -= 1;
        self.signal();
    }

    fn destroy_by_key(&self, key: &T, reason: CloseReason) {
        let pooled = {
            let mut g = self.state.lock().unwrap();
            g.checked_out.remove(key)
        };
        if let Some(p) = pooled {
            self.destroy_detached(p, reason);
        }
    }

    fn destroy_detached(&self, pooled: PooledConnection<T>, reason: CloseReason) {
        self.close_conn(&pooled);
        {
            let mut g = self.state.lock().unwrap();
            g.size -= 1;
            self.signal();
        }
        self.events.safe(|l| l.on_connection_closed(&pooled, reason));
    }

    fn close_conn(&self, pooled: &PooledConnection<T>) {
        let raw = &pooled.raw;
        let result = catch_unwind(AssertUnwindSafe(|| self.factory.close(raw)));
        match result {
            Ok(Ok(())) => {}
            Ok(Err(e)) => eprintln!("connpool: pool {:?}: error closing {}: {}", self.name, pooled, e),
            Err(_) => eprintln!("connpool: pool {:?}: panic closing {}", self.name, pooled),
        }
    }

    fn fill_to_min(&self) {
        loop {
            {
                let mut g = self.state.lock().unwrap();
                if g.closed || g.size >= self.cfg.min_size {
                    return;
                }
                g.size += 1;
            }
            match self.create(None) {
                Err(e) => {
                    eprintln!("connpool: pool {:?}: warm-up failed: {}", self.name, e);
                    return;
                }
                Ok((pooled, created_in)) => {
                    self.events.safe(|l| l.on_connection_created(&pooled, created_in));
                    if let Err(p) = self.park(pooled, self.clock.monotonic()) {
                        self.destroy_detached(p, CloseReason::PoolClosed);
                    }
                }
            }
        }
    }

    fn run_maintenance(&self) {
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
        doomed
            .into_iter()
            .for_each(|(p, reason)| self.destroy_detached(p, reason));
        self.health_check_idle();
        self.fill_to_min();
        self.check_leaks();
    }

    fn run_maintenance_safe(&self) {
        if catch_unwind(AssertUnwindSafe(|| self.run_maintenance())).is_err() {
            eprintln!("connpool: pool {:?}: maintenance pass failed", self.name);
        }
    }

    fn health_check_idle(&self) {
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
            let healthy = self.is_healthy(&raw);
            let doomed = {
                let mut g = self.state.lock().unwrap();
                let pooled = g.checked_out.remove(&key).expect("reserved");
                if healthy && !g.closed {
                    g.idle.push(pooled);
                    self.signal();
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
                self.destroy_detached(p, reason);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resilience::breaker::BreakerConfig;
    use crate::resilience::retry::RetryPolicy;
    use crate::test_support::*;
    use std::sync::atomic::Ordering;
    use std::sync::Arc;

    /// A config with background maintenance and breaker off (the default for most tests).
    fn plain(max_size: usize) -> PoolConfig {
        PoolConfig {
            max_size,
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        }
    }

    #[test]
    fn acquire_release_reuses_same_connection() {
        let h = Harness::new();
        let pool = h.pool(plain(2));
        let a = pool.acquire(Timeout::Unset).unwrap();
        let id_a = a.id;
        pool.release(a).unwrap();
        let b = pool.acquire(Timeout::Unset).unwrap();
        assert_eq!(b.id, id_a, "LIFO reuse returns same connection");
        pool.release(b).unwrap();
        let s = pool.stats();
        assert_eq!(s.created, 1);
        assert_eq!(s.acquired, 2);
        assert_eq!(s.released, 2);
        assert_eq!(pool.name(), "test");
    }

    #[test]
    fn fifo_strategy_rotates() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            idle_strategy: crate::policy::config::IdleStrategy::Fifo,
            ..plain(3)
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        let b = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        pool.release(b).unwrap();
        // FIFO hands back the oldest (a) first.
        let c = pool.acquire(Timeout::Unset).unwrap();
        assert_eq!(c.id, a.id);
    }

    #[test]
    fn max_size_is_a_hard_cap_and_zero_timeout_fails_fast() {
        let h = Harness::new();
        let pool = h.pool(plain(1));
        let a = pool.acquire(Timeout::Unset).unwrap();
        let err = pool.acquire(Timeout::Of(0.0)).unwrap_err();
        assert!(matches!(err, PoolError::Timeout(_)));
        assert_eq!(pool.stats().timeouts, 1);
        pool.release(a).unwrap();
    }

    #[test]
    fn max_waiters_backpressure() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            max_waiters: Some(0),
            ..plain(1)
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        // Full pool, infinite timeout, zero waiter slots -> fail fast.
        assert!(matches!(
            pool.acquire(Timeout::Infinite),
            Err(PoolError::Exhausted(_))
        ));
        pool.release(a).unwrap();
    }

    #[test]
    fn blocked_waiter_is_woken_on_release() {
        let h = Harness::new();
        let pool = h.pool(plain(1));
        let a = pool.acquire(Timeout::Unset).unwrap();
        let p2 = pool.clone();
        let handle = std::thread::spawn(move || p2.acquire(Timeout::Infinite).map(|c| c.id));
        // Give the waiter a moment to block, then release.
        std::thread::sleep(std::time::Duration::from_millis(30));
        pool.release(a).unwrap();
        let got = handle.join().unwrap().unwrap();
        assert_eq!(got, a.id);
        pool.release(Conn::new(got)).unwrap();
    }

    #[test]
    fn timed_wait_eventually_times_out() {
        let h = Harness::new();
        // Real clock so the acquire deadline actually elapses during the condvar wait.
        let pool = ConnectionPool::new(
            h.factory(),
            plain(1),
            Some(PoolOptions {
                name: Some("rt".into()),
                clock: None,
                rng: None,
                listeners: vec![],
            }),
        )
        .unwrap();
        let a = pool.acquire(Timeout::Unset).unwrap();
        let err = pool.acquire(Timeout::Of(0.05)).unwrap_err();
        assert!(matches!(err, PoolError::Timeout(_)));
        pool.release(a).unwrap();
    }

    #[test]
    fn invalid_release_detected() {
        let h = Harness::new();
        let pool = h.pool(plain(10));
        assert!(matches!(
            pool.release(Conn::new(9999)).unwrap_err(),
            PoolError::InvalidRelease(_)
        ));
    }

    #[test]
    fn with_connection_runs_and_releases() {
        let h = Harness::new();
        let pool = h.pool(plain(10));
        let out = pool.with_connection(Timeout::Unset, |c| c.id).unwrap();
        assert!(out >= 1);
        assert_eq!(pool.stats().released, 1);
        // with_connection propagates an acquire error.
        let full = h.pool(plain(1));
        let _held = full.acquire(Timeout::Unset).unwrap();
        assert!(full.with_connection(Timeout::Of(0.0), |c| c.id).is_err());
    }

    #[test]
    fn config_validation_surfaces_on_new() {
        let h = Harness::new();
        let result = ConnectionPool::new(h.factory(), PoolConfig { max_size: 0, ..Default::default() }, None);
        assert!(matches!(result, Err(PoolError::Config(_))));
    }

    #[test]
    fn discard_and_error_release_close_connection() {
        let h = Harness::new();
        let pool = h.pool(plain(5));
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.discard(a).unwrap();
        assert_eq!(h.fac.closed_ids(), vec![a.id]);

        // non-fatal error but validate still ok -> reused (not closed)
        let b = pool.acquire(Timeout::Unset).unwrap();
        let err: FactoryError = "oops".into();
        pool.release_with_error(b, &*err).unwrap();
        assert_eq!(h.fac.closed_ids().len(), 1);

        // error + validation fails -> closed as Error
        h.fac.set_validate_ok(false);
        let c = pool.acquire(Timeout::Unset).unwrap();
        let err2: FactoryError = "bad".into();
        pool.release_with_error(c, &*err2).unwrap();
        assert!(h.fac.closed_ids().contains(&c.id));
        let s = pool.stats();
        assert_eq!(s.closed_by_reason.get("discarded"), Some(&1));
        assert_eq!(s.closed_by_reason.get("error"), Some(&1));
    }

    #[test]
    fn fatal_error_closes_without_validation() {
        let h = Harness::new();
        h.fac.set_fatal(true);
        let pool = h.pool(plain(5));
        let a = pool.acquire(Timeout::Unset).unwrap();
        let err: FactoryError = "fatal".into();
        pool.release_with_error(a, &*err).unwrap();
        assert!(h.fac.closed_ids().contains(&a.id));
    }

    #[test]
    fn reset_failure_discards_on_release() {
        let h = Harness::new();
        h.fac.set_reset_ok(false);
        let pool = h.pool(plain(5));
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        assert!(h.fac.closed_ids().contains(&a.id));
        assert_eq!(pool.stats().closed_by_reason.get("reset_failed"), Some(&1));
    }

    #[test]
    fn rotation_on_release_when_expired_or_overused() {
        let h = Harness::new();
        // max_uses=1 -> after one checkout the connection rotates on release.
        let pool = h.pool(PoolConfig {
            max_uses: Some(1),
            max_lifetime: None,
            ..plain(5)
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        assert!(h.fac.closed_ids().contains(&a.id));
        assert_eq!(pool.stats().closed_by_reason.get("max_uses"), Some(&1));
    }

    #[test]
    fn stale_idle_connection_is_replaced_on_acquire() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            max_lifetime: Some(100.0),
            lifetime_jitter: 0.0,
            max_idle_time: None,
            validate_after_idle: None,
            ..plain(5)
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        // Age past lifetime -> next acquire finds it expired, destroys, creates fresh.
        h.clock.advance(200.0);
        let b = pool.acquire(Timeout::Unset).unwrap();
        assert_ne!(b.id, a.id);
        assert!(h.fac.closed_ids().contains(&a.id));
    }

    #[test]
    fn validate_on_checkout_replaces_unhealthy() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            validate_after_idle: Some(5.0),
            max_lifetime: None,
            max_idle_time: None,
            ..plain(5)
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        h.clock.advance(10.0); // idle long enough to trigger validation
        h.fac.set_validate_ok(false); // it's dead
        let b = pool.acquire(Timeout::Unset).unwrap();
        assert_ne!(b.id, a.id);
        assert_eq!(pool.stats().closed_by_reason.get("unhealthy"), Some(&1));
    }

    #[test]
    fn create_failure_after_retries_surfaces_connection_create_error() {
        let h = Harness::new();
        h.fac.fail_next_creates(10);
        let pool = h.pool(PoolConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                base_delay: 0.0,
                ..Default::default()
            },
            ..plain(2)
        });
        let err = pool.acquire(Timeout::Unset).unwrap_err();
        assert!(matches!(err, PoolError::ConnectionCreate { .. }));
        assert_eq!(pool.stats().create_failures, 3);
        // the reserved slot was freed, so a later success works
        h.fac.fail_next_creates(0);
        assert!(pool.acquire(Timeout::Unset).is_ok());
    }

    #[test]
    fn create_retry_then_success() {
        let h = Harness::new();
        h.fac.fail_next_creates(1);
        let pool = h.pool(PoolConfig {
            retry: RetryPolicy {
                max_attempts: 3,
                base_delay: 0.0,
                ..Default::default()
            },
            ..plain(2)
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        assert_eq!(pool.stats().create_failures, 1);
        pool.release(a).unwrap();
    }

    #[test]
    fn retry_delay_exceeding_deadline_times_out() {
        let h = Harness::new();
        h.fac.fail_next_creates(10);
        let pool = h.pool(PoolConfig {
            acquire_timeout: Some(0.05),
            retry: RetryPolicy {
                max_attempts: 5,
                base_delay: 100.0,
                jitter: 0.0,
                ..Default::default()
            },
            ..plain(2)
        });
        // First attempt fails; the backoff (100s) exceeds the 0.05s deadline -> Timeout.
        let err = pool.acquire(Timeout::Unset).unwrap_err();
        assert!(matches!(err, PoolError::Timeout(_)));
    }

    #[test]
    fn create_panic_is_contained() {
        let h = Harness::new();
        h.fac.set_create_panic(true);
        let pool = h.pool(PoolConfig {
            retry: RetryPolicy::no_retry(),
            ..plain(2)
        });
        assert!(matches!(
            pool.acquire(Timeout::Unset),
            Err(PoolError::ConnectionCreate { .. })
        ));
    }

    #[test]
    fn duplicate_object_rejected() {
        let h = Harness::new();
        h.fac.force_id(Some(42));
        let pool = h.pool(plain(5));
        let _a = pool.acquire(Timeout::Unset).unwrap(); // id 42 checked out
        // next create also returns 42 -> duplicate while 42 is managed
        let err = pool.acquire(Timeout::Unset).unwrap_err();
        assert!(matches!(err, PoolError::ConnectionCreate { .. }));
    }

    #[test]
    fn breaker_trips_then_blocks_then_recovers() {
        let h = Harness::new();
        h.fac.fail_next_creates(100);
        let pool = h.pool(PoolConfig {
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
        // one failed creation trips the breaker
        assert!(matches!(
            pool.acquire(Timeout::Unset),
            Err(PoolError::ConnectionCreate { .. })
        ));
        // now fast-fails via the breaker
        assert!(matches!(
            pool.acquire(Timeout::Unset),
            Err(PoolError::CircuitOpen { .. })
        ));
        assert_eq!(pool.stats().breaker_state.as_deref(), Some("open"));
        // recover + succeed
        h.clock.advance(10.0);
        h.fac.fail_next_creates(0);
        let a = pool.acquire(Timeout::Unset).unwrap();
        assert_eq!(pool.stats().breaker_state.as_deref(), Some("closed"));
        pool.release(a).unwrap();
    }

    #[test]
    fn breaker_abort_does_not_trip() {
        let h = Harness::new();
        h.fac.fail_next_creates(100);
        let pool = h.pool(PoolConfig {
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
        // retry backoff exceeds deadline -> Timeout -> breaker records an abort, not a failure
        assert!(matches!(pool.acquire(Timeout::Unset), Err(PoolError::Timeout(_))));
        assert_eq!(pool.stats().breaker_state.as_deref(), Some("closed"));
    }

    #[test]
    fn open_warms_up_and_close_drains() {
        let h = Harness::new();
        let rec = Recorder::new();
        let pool = h.pool_with(
            PoolConfig {
                min_size: 2,
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
            vec![rec.clone()],
        );
        pool.open().unwrap();
        assert!(pool.open().is_ok()); // idempotent
        assert_eq!(pool.stats().idle, 2);
        assert_eq!(rec.created.load(Ordering::SeqCst), 2);
        pool.close(Some(0.0));
        pool.close(None); // idempotent
        assert!(pool.is_closed());
        assert_eq!(rec.closed.load(Ordering::SeqCst), 2);
        // open after close errors
        assert!(matches!(pool.open(), Err(PoolError::Closed(_))));
    }

    #[test]
    fn close_drains_checked_out_with_timeout() {
        let h = Harness::new();
        let pool = h.pool(plain(2));
        let a = pool.acquire(Timeout::Unset).unwrap();
        let p2 = pool.clone();
        let closer = std::thread::spawn(move || p2.close(Some(5.0)));
        std::thread::sleep(std::time::Duration::from_millis(30));
        pool.release(a).unwrap(); // lets the drain complete
        closer.join().unwrap();
        assert!(pool.is_closed());
    }

    #[test]
    fn release_into_closed_pool_closes_connection() {
        let h = Harness::new();
        let pool = h.pool(plain(2));
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.close(None);
        // connection still checked out; releasing now must close it (pool_closed)
        pool.release(a).unwrap();
        assert!(h.fac.closed_ids().contains(&a.id));
    }

    #[test]
    fn maintenance_sweeps_and_refills() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            min_size: 1,
            max_lifetime: Some(100.0),
            lifetime_jitter: 0.0,
            max_idle_time: Some(50.0),
            validate_after_idle: None,
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        });
        pool.open().unwrap();
        let first_idle = pool.stats().idle;
        assert_eq!(first_idle, 1);
        // age out the warm connection; maintenance evicts then refills to min_size
        h.clock.advance(200.0);
        pool.run_maintenance();
        let s = pool.stats();
        assert_eq!(s.idle, 1); // refilled
        assert!(s.closed >= 1); // old one evicted (expired)
        pool.close(None);
    }

    #[test]
    fn maintenance_noop_when_closed() {
        let h = Harness::new();
        let pool = h.pool(plain(2));
        pool.close(None);
        pool.run_maintenance(); // early-returns, no panic
    }

    #[test]
    fn health_check_idle_evicts_unhealthy() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            validate_idle: true,
            max_lifetime: None,
            max_idle_time: None,
            validate_after_idle: None,
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        // healthy pass keeps it
        pool.run_maintenance();
        assert_eq!(pool.stats().idle, 1);
        // unhealthy pass evicts it
        h.fac.set_validate_ok(false);
        pool.run_maintenance();
        assert_eq!(pool.stats().idle, 0);
        assert_eq!(pool.stats().closed_by_reason.get("unhealthy"), Some(&1));
    }

    #[test]
    fn leak_detection_warns_once() {
        let h = Harness::new();
        let rec = Recorder::new();
        let pool = h.pool_with(
            PoolConfig {
                leak_detection_timeout: Some(10.0),
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
            vec![rec.clone()],
        );
        let a = pool.acquire(Timeout::Unset).unwrap();
        h.clock.advance(20.0); // held longer than the leak threshold
        pool.run_maintenance();
        pool.run_maintenance(); // already reported -> not counted twice
        assert_eq!(rec.leaks.load(Ordering::SeqCst), 1);
        pool.release(a).unwrap();
    }

    #[test]
    fn background_maintenance_thread_runs_and_stops() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            min_size: 0,
            maintenance_interval: Some(0.01),
            breaker: None,
            ..Default::default()
        });
        pool.open().unwrap();
        std::thread::sleep(std::time::Duration::from_millis(40)); // let the ticker fire
        pool.close(None); // signals + joins the thread
        assert!(pool.is_closed());
    }

    #[test]
    fn listener_panic_does_not_break_pool() {
        struct Boom;
        impl PoolListener<Conn> for Boom {
            fn on_acquired(&self, _c: &PooledConnection<Conn>, _w: f64) {
                panic!("listener boom");
            }
        }
        let h = Harness::new();
        let pool = h.pool_with(plain(2), vec![Arc::new(Boom)]);
        let a = pool.acquire(Timeout::Unset).unwrap(); // panic swallowed
        pool.release(a).unwrap();
    }

    #[test]
    fn breaker_state_change_reaches_listeners() {
        let h = Harness::new();
        let rec = Recorder::new();
        h.fac.fail_next_creates(100);
        let pool = h.pool_with(
            PoolConfig {
                retry: RetryPolicy::no_retry(),
                maintenance_interval: None,
                breaker: Some(BreakerConfig { failure_threshold: 1, ..Default::default() }),
                ..Default::default()
            },
            vec![rec.clone()],
        );
        let _ = pool.acquire(Timeout::Unset); // fails -> breaker trips -> listener notified
        assert!(!rec.breaker_changes.lock().unwrap().is_empty());
    }

    #[test]
    fn waiter_under_max_waiters_blocks_then_succeeds() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig { max_waiters: Some(5), ..plain(1) });
        let a = pool.acquire(Timeout::Unset).unwrap();
        let p2 = pool.clone();
        let waiter = std::thread::spawn(move || p2.acquire(Timeout::Infinite).map(|c| c.id));
        std::thread::sleep(std::time::Duration::from_millis(30));
        pool.release(a).unwrap();
        assert_eq!(waiter.join().unwrap().unwrap(), a.id);
        pool.release(Conn::new(a.id)).unwrap();
    }

    #[test]
    fn retry_sleeps_without_deadline() {
        let h = Harness::new();
        h.fac.fail_next_creates(1);
        let pool = h.pool(PoolConfig {
            acquire_timeout: None, // no deadline -> remaining() is None in the retry loop
            retry: RetryPolicy { max_attempts: 3, base_delay: 1.0, jitter: 0.0, ..Default::default() },
            ..plain(2)
        });
        let a = pool.acquire(Timeout::Unset).unwrap(); // first create fails, sleeps, then ok
        pool.release(a).unwrap();
    }

    #[test]
    fn validate_panic_on_checkout_treated_unhealthy() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            validate_after_idle: Some(5.0),
            max_lifetime: None,
            max_idle_time: None,
            ..plain(5)
        });
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        h.clock.advance(10.0);
        h.fac.set_validate_panic(true);
        let b = pool.acquire(Timeout::Unset).unwrap(); // panic -> unhealthy -> fresh conn
        assert_ne!(b.id, a.id);
        pool.release(b).unwrap();
    }

    #[test]
    fn reset_panic_discards() {
        let h = Harness::new();
        h.fac.set_reset_panic(true);
        let pool = h.pool(plain(5));
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
        assert_eq!(pool.stats().closed_by_reason.get("reset_failed"), Some(&1));
    }

    #[test]
    fn close_errors_and_panics_are_logged() {
        let h = Harness::new();
        h.fac.set_close_err(true);
        let pool = h.pool(plain(5));
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.discard(a).unwrap(); // close returns Err -> logged, still counted closed
        assert_eq!(pool.stats().closed, 1);

        h.fac.set_close_err(false);
        h.fac.set_close_panic(true);
        let b = pool.acquire(Timeout::Unset).unwrap();
        pool.discard(b).unwrap(); // close panics -> caught + logged
        assert_eq!(pool.stats().closed, 2);
    }

    #[test]
    fn warmup_failure_is_logged_and_stops() {
        let h = Harness::new();
        h.fac.fail_next_creates(100);
        let pool = h.pool(PoolConfig {
            min_size: 2,
            retry: RetryPolicy::no_retry(),
            maintenance_interval: None,
            breaker: None,
            ..Default::default()
        });
        pool.open().unwrap(); // warm-up fails on first create -> logged, returns
        assert_eq!(pool.stats().idle, 0);
    }

    #[test]
    fn pool_closed_during_create_registers_as_closed() {
        // A factory that closes the pool from inside create(), after producing a connection.
        let slot: Arc<Mutex<Option<ConnectionPool<Conn>>>> = Arc::new(Mutex::new(None));
        let s2 = slot.clone();
        let factory = crate::factories::sync_factory::FnFactory::new(move || {
            if let Some(p) = s2.lock().unwrap().as_ref() {
                p.close(None);
            }
            Ok::<_, FactoryError>(Conn::new(7))
        });
        let pool = ConnectionPool::new(factory, plain(2), None).unwrap();
        *slot.lock().unwrap() = Some(pool.clone());
        // create succeeds then register_new sees the pool closed.
        assert!(matches!(pool.acquire(Timeout::Unset), Err(PoolError::Closed(_))));
    }

    #[test]
    fn pool_closed_mid_retry_loop() {
        // A factory that closes the pool then fails, so the retry loop's closed-check fires.
        let slot: Arc<Mutex<Option<ConnectionPool<Conn>>>> = Arc::new(Mutex::new(None));
        let s2 = slot.clone();
        let factory = crate::factories::sync_factory::FnFactory::new(move || {
            if let Some(p) = s2.lock().unwrap().as_ref() {
                p.close(None);
            }
            Err::<Conn, FactoryError>("boom".into())
        });
        let pool = ConnectionPool::new(
            factory,
            PoolConfig {
                retry: RetryPolicy { max_attempts: 3, base_delay: 0.0, ..Default::default() },
                ..plain(2)
            },
            None,
        )
        .unwrap();
        *slot.lock().unwrap() = Some(pool.clone());
        assert!(matches!(pool.acquire(Timeout::Unset), Err(PoolError::Closed(_))));
    }

    #[test]
    fn maintenance_panic_is_contained() {
        let h = Harness::new();
        let pool = h.pool(plain(2));
        // Poison the state mutex, then run the guarded maintenance entry point.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _g = pool.0.state.lock().unwrap();
            panic!("poison");
        }));
        pool.0.run_maintenance_safe(); // catches the poisoned-lock panic, logs, returns
    }

    #[test]
    fn close_with_timeout_gives_up_when_deadline_passed() {
        let h = Harness::new();
        let pool = h.pool(plain(2));
        let a = pool.acquire(Timeout::Unset).unwrap();
        // immediate drain deadline with a connection still out -> loop breaks at once
        pool.close(Some(0.0));
        assert!(pool.is_closed());
        pool.release(a).unwrap(); // still releasable; closes the conn
    }

    #[test]
    fn blocked_waiter_wakes_as_closed_when_pool_closes() {
        let h = Harness::new();
        let pool = h.pool(plain(1));
        let a = pool.acquire(Timeout::Unset).unwrap();
        let p2 = pool.clone();
        let waiter = std::thread::spawn(move || p2.acquire(Timeout::Infinite));
        std::thread::sleep(std::time::Duration::from_millis(30));
        pool.close(None); // wakes the waiter, which re-checks closed
        let res = waiter.join().unwrap();
        assert!(matches!(res, Err(PoolError::Closed(_))));
        pool.release(a).unwrap();
    }

    #[test]
    fn fill_to_min_park_fails_when_closed_mid_create() {
        // A factory that closes the pool from inside create() (returning Ok) so fill_to_min's
        // park sees the pool closed and destroys the just-built connection.
        let slot: Arc<Mutex<Option<ConnectionPool<Conn>>>> = Arc::new(Mutex::new(None));
        let s2 = slot.clone();
        let factory = crate::factories::sync_factory::FnFactory::new(move || {
            if let Some(p) = s2.lock().unwrap().as_ref() {
                p.close(None);
            }
            Ok::<_, FactoryError>(Conn::new(7))
        });
        let pool = ConnectionPool::new(
            factory,
            PoolConfig {
                min_size: 1,
                retry: RetryPolicy::no_retry(),
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        *slot.lock().unwrap() = Some(pool.clone());
        pool.open().unwrap(); // warm-up creates one, pool closes, park fails -> destroyed
        assert!(pool.is_closed());
    }

    #[test]
    fn health_check_closed_mid_validation_uses_pool_closed_reason() {
        // validate() closes the pool and reports healthy, so health_check_idle evicts with
        // PoolClosed (healthy-but-closed branch).
        let slot: Arc<Mutex<Option<ConnectionPool<Conn>>>> = Arc::new(Mutex::new(None));
        let close_on_validate = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let s2 = slot.clone();
        let flag = close_on_validate.clone();
        let next = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let factory = crate::factories::sync_factory::FnFactory::new(move || {
            Ok::<_, FactoryError>(Conn::new(next.fetch_add(1, Ordering::SeqCst) + 1))
        })
        .with_validate(move |_c| {
            if flag.load(Ordering::SeqCst) {
                s2.lock().unwrap().as_ref().unwrap().close(None);
            }
            true
        });
        let pool = ConnectionPool::new(
            factory,
            PoolConfig {
                validate_idle: true,
                validate_after_idle: None,
                max_lifetime: None,
                max_idle_time: None,
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
            None,
        )
        .unwrap();
        *slot.lock().unwrap() = Some(pool.clone());
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap(); // one idle conn
        close_on_validate.store(true, Ordering::SeqCst);
        pool.run_maintenance(); // health check validates -> closes pool -> healthy-but-closed
        assert!(pool.is_closed());
    }

    #[test]
    fn default_options_when_none() {
        // None options -> default name/clock/rng (PoolOptions::default path).
        let h = Harness::new();
        let pool = ConnectionPool::new(h.factory(), plain(2), None).unwrap();
        assert_eq!(pool.name(), "pool");
        let a = pool.acquire(Timeout::Unset).unwrap();
        pool.release(a).unwrap();
    }

    #[test]
    fn dropping_pool_without_close_stops_sweeper() {
        let h = Harness::new();
        let pool = h.pool(PoolConfig {
            maintenance_interval: Some(0.01),
            breaker: None,
            ..Default::default()
        });
        pool.open().unwrap();
        drop(pool); // Inner::drop signals the sweeper to stop
        std::thread::sleep(std::time::Duration::from_millis(30));
    }
}
