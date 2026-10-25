//! `PooledConnection`: a raw connection plus the metadata the pool needs to manage it, and
//! `CloseReason`: why a connection left the pool.

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// Why a connection left the pool. The string values ([`CloseReason::as_str`]) are **wire
/// values** kept identical across every poolyglot language — do not localize them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CloseReason {
    /// `max_lifetime` reached (rotation).
    Expired,
    /// `max_uses` reached (rotation).
    MaxUses,
    /// Sat idle longer than `max_idle_time`.
    Idle,
    /// Failed validation.
    Unhealthy,
    /// The caller's work raised and the connection is suspect.
    Error,
    /// `reset()` failed on release.
    ResetFailed,
    /// The caller asked for it to be discarded.
    Discarded,
    /// The pool was closed.
    PoolClosed,
}

impl CloseReason {
    /// The snake_case wire string for this reason.
    pub fn as_str(&self) -> &'static str {
        match self {
            CloseReason::Expired => "expired",
            CloseReason::MaxUses => "max_uses",
            CloseReason::Idle => "idle",
            CloseReason::Unhealthy => "unhealthy",
            CloseReason::Error => "error",
            CloseReason::ResetFailed => "reset_failed",
            CloseReason::Discarded => "discarded",
            CloseReason::PoolClosed => "pool_closed",
        }
    }
}

impl fmt::Display for CloseReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

static CONN_IDS: AtomicU64 = AtomicU64::new(0);

/// A raw connection plus pool bookkeeping. Instances are tracked by the pool; listeners
/// receive `&PooledConnection<T>`. The raw connection is reachable via [`PooledConnection::raw`].
#[derive(Debug, Clone)]
pub struct PooledConnection<T> {
    pub(crate) raw: T,
    pub(crate) created_at: f64,
    pub(crate) expires_at: Option<f64>,
    pub(crate) last_returned_at: Option<f64>,
    pub(crate) checked_out_at: Option<f64>,
    pub(crate) use_count: u64,
    pub(crate) leak_reported: bool,
    pub(crate) id: u64,
}

impl<T> PooledConnection<T> {
    /// Builds a wrapper; `last_returned_at` defaults to `created_at`.
    pub(crate) fn new(raw: T, created_at: f64, expires_at: Option<f64>) -> Self {
        Self {
            raw,
            created_at,
            expires_at,
            last_returned_at: Some(created_at),
            checked_out_at: None,
            use_count: 0,
            leak_reported: false,
            id: CONN_IDS.fetch_add(1, Ordering::Relaxed) + 1,
        }
    }

    /// The wrapped connection.
    pub fn raw(&self) -> &T {
        &self.raw
    }

    /// This connection's unique id (monotonic across the process).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// How many times this connection has been checked out.
    pub fn use_count(&self) -> u64 {
        self.use_count
    }

    /// When the connection was created (clock seconds).
    pub fn created_at(&self) -> f64 {
        self.created_at
    }

    /// Age in seconds at `now`.
    pub fn age(&self, now: f64) -> f64 {
        now - self.created_at
    }

    /// Seconds spent idle since last returned (or created), at `now`.
    pub fn idle_time(&self, now: f64) -> f64 {
        now - self.last_returned_at.unwrap_or(self.created_at)
    }

    /// Seconds held since checkout at `now`, or `0.0` if not checked out.
    pub fn hold_time(&self, now: f64) -> f64 {
        match self.checked_out_at {
            Some(t) => now - t,
            None => 0.0,
        }
    }

    pub(crate) fn mark_checked_out(&mut self, now: f64) {
        self.checked_out_at = Some(now);
        self.use_count += 1;
        self.leak_reported = false;
    }

    pub(crate) fn mark_returned(&mut self, now: f64) {
        self.checked_out_at = None;
        self.last_returned_at = Some(now);
    }
}

impl<T> fmt::Display for PooledConnection<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<PooledConnection #{} uses={}>", self.id, self.use_count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_reason_wire_values() {
        assert_eq!(CloseReason::Expired.as_str(), "expired");
        assert_eq!(CloseReason::MaxUses.as_str(), "max_uses");
        assert_eq!(CloseReason::Idle.as_str(), "idle");
        assert_eq!(CloseReason::Unhealthy.as_str(), "unhealthy");
        assert_eq!(CloseReason::Error.as_str(), "error");
        assert_eq!(CloseReason::ResetFailed.as_str(), "reset_failed");
        assert_eq!(CloseReason::Discarded.as_str(), "discarded");
        assert_eq!(CloseReason::PoolClosed.as_str(), "pool_closed");
        assert_eq!(CloseReason::Idle.to_string(), "idle");
        assert_eq!(format!("{:?}", CloseReason::Idle), "Idle");
    }

    #[test]
    fn pooled_connection_lifecycle() {
        let mut c = PooledConnection::new("conn", 100.0, Some(150.0));
        assert_eq!(*c.raw(), "conn");
        assert!(c.id() >= 1);
        assert_eq!(c.use_count(), 0);
        assert_eq!(c.created_at(), 100.0);
        assert_eq!(c.age(110.0), 10.0);
        assert_eq!(c.idle_time(105.0), 5.0); // last_returned defaults to created_at
        assert_eq!(c.hold_time(105.0), 0.0); // not checked out

        c.mark_checked_out(120.0);
        assert_eq!(c.use_count(), 1);
        assert_eq!(c.hold_time(130.0), 10.0);

        c.mark_returned(140.0);
        assert_eq!(c.hold_time(150.0), 0.0);
        assert_eq!(c.idle_time(145.0), 5.0);

        assert!(c.to_string().starts_with("<PooledConnection #"));
        assert!(format!("{c:?}").contains("PooledConnection"));
    }

    #[test]
    fn ids_are_distinct_and_monotonic() {
        let a = PooledConnection::new((), 0.0, None);
        let b = PooledConnection::new((), 0.0, None);
        assert!(b.id() > a.id());
        // idle_time falls back to created_at when last_returned is cleared.
        let mut c = PooledConnection::new((), 10.0, None);
        c.last_returned_at = None;
        assert_eq!(c.idle_time(25.0), 15.0);
    }
}
