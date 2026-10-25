//! `PoolStats`: an immutable metrics snapshot, and the collector that feeds it.

use std::collections::BTreeMap;
use std::error::Error;
use std::sync::Mutex;

use crate::core::connection::{CloseReason, PooledConnection};
use crate::observability::events::PoolListener;
use crate::resilience::breaker::BreakerState;

/// Caps how many recent acquire-wait / create-latency samples are kept. avg and all
/// percentiles are computed over this same window, so they always describe the same bounded
/// population.
const ACQUIRE_WAIT_WINDOW: usize = 1024;

/// A value in the [`PoolStats::to_dictionary`] wire representation.
#[derive(Debug, Clone, PartialEq)]
pub enum StatValue {
    /// A string value.
    Str(String),
    /// An integer value.
    Int(i64),
    /// A floating-point value.
    Float(f64),
    /// A map of close-reason -> count.
    Map(BTreeMap<String, i64>),
    /// A null value (used for an absent breaker state).
    Null,
}

/// Linear-interpolated percentile of an already-sorted slice. `q` in `[0, 1]`. Empty -> `0.0`.
fn percentile(sorted: &[f64], q: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let k = (sorted.len() - 1) as f64 * q;
    let lo = k as usize;
    let hi = (lo + 1).min(sorted.len() - 1);
    sorted[lo] + (sorted[hi] - sorted[lo]) * (k - lo as f64)
}

fn average(values: &[f64]) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    values.iter().sum::<f64>() / values.len() as f64
}

/// An immutable snapshot of pool metrics. The keys of [`PoolStats::to_dictionary`] are
/// snake_case wire strings identical across every poolyglot language.
#[derive(Debug, Clone, PartialEq)]
pub struct PoolStats {
    /// Pool name.
    pub name: String,
    /// Configured hard cap.
    pub max_size: usize,
    /// idle + in use + being created.
    pub size: usize,
    /// Available for checkout right now.
    pub idle: usize,
    /// Checked out, or briefly reserved by a background idle check.
    pub in_use: usize,
    /// Callers currently blocked in `acquire()`.
    pub waiting: usize,

    /// Total connections created.
    pub created: u64,
    /// Total create failures.
    pub create_failures: u64,
    /// Total acquires.
    pub acquired: u64,
    /// Total releases.
    pub released: u64,
    /// Total acquire timeouts.
    pub timeouts: u64,
    /// Total connections closed.
    pub closed: u64,
    /// Borrowed connections flagged as possibly leaked.
    pub leaks: u64,
    /// Close counts by reason wire string.
    pub closed_by_reason: BTreeMap<String, i64>,

    /// Average acquire-wait seconds over the window.
    pub avg_acquire_wait: f64,
    /// p50 acquire-wait.
    pub p50_acquire_wait: f64,
    /// p95 acquire-wait.
    pub p95_acquire_wait: f64,
    /// p99 acquire-wait.
    pub p99_acquire_wait: f64,

    /// Average create-latency seconds over the window.
    pub avg_create_latency: f64,
    /// p50 create-latency.
    pub p50_create_latency: f64,
    /// p95 create-latency.
    pub p95_create_latency: f64,
    /// p99 create-latency.
    pub p99_create_latency: f64,

    /// Current breaker state wire string, if a breaker is enabled.
    pub breaker_state: Option<String>,
}

impl PoolStats {
    /// The snake_case wire representation, identical across every poolyglot language.
    pub fn to_dictionary(&self) -> BTreeMap<&'static str, StatValue> {
        let mut m = BTreeMap::new();
        m.insert("name", StatValue::Str(self.name.clone()));
        m.insert("max_size", StatValue::Int(self.max_size as i64));
        m.insert("size", StatValue::Int(self.size as i64));
        m.insert("idle", StatValue::Int(self.idle as i64));
        m.insert("in_use", StatValue::Int(self.in_use as i64));
        m.insert("waiting", StatValue::Int(self.waiting as i64));
        m.insert("created", StatValue::Int(self.created as i64));
        m.insert(
            "create_failures",
            StatValue::Int(self.create_failures as i64),
        );
        m.insert("acquired", StatValue::Int(self.acquired as i64));
        m.insert("released", StatValue::Int(self.released as i64));
        m.insert("timeouts", StatValue::Int(self.timeouts as i64));
        m.insert("closed", StatValue::Int(self.closed as i64));
        m.insert("leaks", StatValue::Int(self.leaks as i64));
        m.insert(
            "closed_by_reason",
            StatValue::Map(self.closed_by_reason.clone()),
        );
        m.insert("avg_acquire_wait", StatValue::Float(self.avg_acquire_wait));
        m.insert("p50_acquire_wait", StatValue::Float(self.p50_acquire_wait));
        m.insert("p95_acquire_wait", StatValue::Float(self.p95_acquire_wait));
        m.insert("p99_acquire_wait", StatValue::Float(self.p99_acquire_wait));
        m.insert(
            "avg_create_latency",
            StatValue::Float(self.avg_create_latency),
        );
        m.insert(
            "p50_create_latency",
            StatValue::Float(self.p50_create_latency),
        );
        m.insert(
            "p95_create_latency",
            StatValue::Float(self.p95_create_latency),
        );
        m.insert(
            "p99_create_latency",
            StatValue::Float(self.p99_create_latency),
        );
        m.insert(
            "breaker_state",
            match &self.breaker_state {
                Some(s) => StatValue::Str(s.clone()),
                None => StatValue::Null,
            },
        );
        m
    }
}

/// The point-in-time gauges a collector doesn't itself track.
pub(crate) struct StatsGauges {
    pub name: String,
    pub max_size: usize,
    pub size: usize,
    pub idle: usize,
    pub in_use: usize,
    pub waiting: usize,
    pub breaker_state: Option<String>,
}

struct Counters {
    created: u64,
    create_failures: u64,
    acquired: u64,
    released: u64,
    timeouts: u64,
    closed: u64,
    leaks: u64,
    by_reason: BTreeMap<String, i64>,
    waits: Vec<f64>,
    creates: Vec<f64>,
}

/// A [`PoolListener`] that counts events and keeps bounded windows of acquire-wait and
/// create-latency samples for percentile reporting.
pub(crate) struct StatsCollector<T> {
    inner: Mutex<Counters>,
    _marker: std::marker::PhantomData<fn(T)>,
}

fn push_window(window: &mut Vec<f64>, value: f64) {
    window.push(value);
    if window.len() > ACQUIRE_WAIT_WINDOW {
        let overflow = window.len() - ACQUIRE_WAIT_WINDOW;
        window.drain(0..overflow);
    }
}

impl<T> StatsCollector<T> {
    pub(crate) fn new() -> Self {
        Self {
            inner: Mutex::new(Counters {
                created: 0,
                create_failures: 0,
                acquired: 0,
                released: 0,
                timeouts: 0,
                closed: 0,
                leaks: 0,
                by_reason: BTreeMap::new(),
                waits: Vec::new(),
                creates: Vec::new(),
            }),
            _marker: std::marker::PhantomData,
        }
    }

    pub(crate) fn snapshot(&self, g: StatsGauges) -> PoolStats {
        let c = self.inner.lock().unwrap();
        let mut waits = c.waits.clone();
        waits.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let mut creates = c.creates.clone();
        creates.sort_by(|a, b| a.partial_cmp(b).unwrap());
        PoolStats {
            name: g.name,
            max_size: g.max_size,
            size: g.size,
            idle: g.idle,
            in_use: g.in_use,
            waiting: g.waiting,
            created: c.created,
            create_failures: c.create_failures,
            acquired: c.acquired,
            released: c.released,
            timeouts: c.timeouts,
            closed: c.closed,
            leaks: c.leaks,
            closed_by_reason: c.by_reason.clone(),
            avg_acquire_wait: average(&waits),
            p50_acquire_wait: percentile(&waits, 0.50),
            p95_acquire_wait: percentile(&waits, 0.95),
            p99_acquire_wait: percentile(&waits, 0.99),
            avg_create_latency: average(&creates),
            p50_create_latency: percentile(&creates, 0.50),
            p95_create_latency: percentile(&creates, 0.95),
            p99_create_latency: percentile(&creates, 0.99),
            breaker_state: g.breaker_state,
        }
    }
}

impl<T> PoolListener<T> for StatsCollector<T> {
    fn on_connection_created(&self, _conn: &PooledConnection<T>, duration: f64) {
        let mut c = self.inner.lock().unwrap();
        c.created += 1;
        push_window(&mut c.creates, duration);
    }

    fn on_connection_closed(&self, _conn: &PooledConnection<T>, reason: CloseReason) {
        let mut c = self.inner.lock().unwrap();
        c.closed += 1;
        *c.by_reason.entry(reason.as_str().to_string()).or_insert(0) += 1;
    }

    fn on_create_failed(&self, _error: &(dyn Error + 'static)) {
        self.inner.lock().unwrap().create_failures += 1;
    }

    fn on_acquired(&self, _conn: &PooledConnection<T>, wait_time: f64) {
        let mut c = self.inner.lock().unwrap();
        c.acquired += 1;
        push_window(&mut c.waits, wait_time);
    }

    fn on_released(&self, _conn: &PooledConnection<T>, _hold_time: f64) {
        self.inner.lock().unwrap().released += 1;
    }

    fn on_acquire_timeout(&self, _waited: f64) {
        self.inner.lock().unwrap().timeouts += 1;
    }

    fn on_connection_leak(&self, _conn: &PooledConnection<T>, _held_for: f64) {
        self.inner.lock().unwrap().leaks += 1;
    }

    fn on_breaker_state_change(&self, _old: BreakerState, _new: BreakerState) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gauges() -> StatsGauges {
        StatsGauges {
            name: "p".into(),
            max_size: 10,
            size: 2,
            idle: 1,
            in_use: 1,
            waiting: 0,
            breaker_state: None,
        }
    }

    #[test]
    fn percentile_and_average_edge_cases() {
        assert_eq!(percentile(&[], 0.5), 0.0);
        assert_eq!(average(&[]), 0.0);
        let v = vec![1.0, 2.0, 3.0, 4.0];
        assert_eq!(percentile(&v, 0.0), 1.0);
        assert_eq!(percentile(&v, 1.0), 4.0);
        assert_eq!(percentile(&v, 0.5), 2.5); // interpolated
        assert_eq!(average(&v), 2.5);
    }

    #[test]
    fn collector_counts_and_snapshots() {
        let c = StatsCollector::<u32>::new();
        let pc = PooledConnection::new(1u32, 0.0, None);
        c.on_connection_created(&pc, 0.2);
        c.on_connection_created(&pc, 0.4);
        c.on_acquired(&pc, 0.1);
        c.on_released(&pc, 1.0);
        c.on_acquire_timeout(2.0);
        c.on_connection_closed(&pc, CloseReason::Idle);
        c.on_connection_closed(&pc, CloseReason::Idle);
        c.on_connection_leak(&pc, 5.0);
        let e: Box<dyn Error + Send + Sync> = "x".into();
        c.on_create_failed(&*e);
        c.on_breaker_state_change(BreakerState::Closed, BreakerState::Open);

        let s = c.snapshot(gauges());
        assert_eq!(s.created, 2);
        assert_eq!(s.acquired, 1);
        assert_eq!(s.released, 1);
        assert_eq!(s.timeouts, 1);
        assert_eq!(s.closed, 2);
        assert_eq!(s.leaks, 1);
        assert_eq!(s.create_failures, 1);
        assert_eq!(s.closed_by_reason.get("idle"), Some(&2));
        assert!((s.avg_create_latency - 0.3).abs() < 1e-9);
        assert_eq!(s.name, "p");
        assert_eq!(s.max_size, 10);
    }

    #[test]
    fn to_dictionary_has_snakecase_keys() {
        let c = StatsCollector::<u32>::new();
        let mut g = gauges();
        g.breaker_state = Some("closed".into());
        let d = c.snapshot(g).to_dictionary();
        for key in [
            "name",
            "max_size",
            "size",
            "idle",
            "in_use",
            "waiting",
            "created",
            "create_failures",
            "acquired",
            "released",
            "timeouts",
            "closed",
            "leaks",
            "closed_by_reason",
            "avg_acquire_wait",
            "p50_acquire_wait",
            "p95_acquire_wait",
            "p99_acquire_wait",
            "avg_create_latency",
            "p50_create_latency",
            "p95_create_latency",
            "p99_create_latency",
            "breaker_state",
        ] {
            assert!(d.contains_key(key), "missing key {key}");
        }
        assert_eq!(d["breaker_state"], StatValue::Str("closed".into()));
        assert_eq!(d["name"], StatValue::Str("p".into()));
        assert_eq!(d["max_size"], StatValue::Int(10));
        // Null breaker when absent.
        let d2 = c.snapshot(gauges()).to_dictionary();
        assert_eq!(d2["breaker_state"], StatValue::Null);
    }

    #[test]
    fn window_is_bounded() {
        let c = StatsCollector::<u32>::new();
        let pc = PooledConnection::new(1u32, 0.0, None);
        for i in 0..(ACQUIRE_WAIT_WINDOW + 50) {
            c.on_acquired(&pc, i as f64);
        }
        let inner = c.inner.lock().unwrap();
        assert_eq!(inner.waits.len(), ACQUIRE_WAIT_WINDOW);
        // oldest 50 dropped -> first sample is 50.0
        assert_eq!(inner.waits[0], 50.0);
    }
}
