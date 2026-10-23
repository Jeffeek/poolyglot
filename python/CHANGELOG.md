# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- **Async cancellation mid-`factory.close()` could skip disposal and unbalance accounting.**
  If a caller was cancelled while a connection was being disposed (e.g. `release(discard=True)`,
  or dropping a stale/expired connection in `acquire`), backend `close()` was interrupted, the
  connection could be left half-open, and the `on_connection_closed` event was never fired - so
  `PoolStats.closed` (and `closed_by_reason`) silently drifted and listeners missed the close.
  All async disposal now runs through a single shielded `_destroy`: the caller still observes
  `CancelledError` immediately, but backend close, the slot decrement, and the close event
  always complete. (Sync pool unaffected - threads are not cancelled mid-call.)
- **Async `open()` racing `close()` leaked the maintenance task.** If `close()` ran to
  completion while `open()` was still awaiting `_fill_to_min()`, `close()` saw no task to
  cancel and `open()` then started a maintenance loop that outlived shutdown - a task that
  never stopped, pinned the pool in memory (so GC and the `ResourceWarning` never fired),
  and warned "Task was destroyed but it is pending" at loop teardown. `open()` now starts
  the task only while the pool is still open. The sync pool was never affected (its
  maintenance thread checks the stop event every cycle).
- **Circuit breaker counted every retry attempt as a separate failure.** A single
  `acquire()` against a down backend could trip a `failure_threshold=5` breaker in ~2
  calls. The breaker now records **one** success/failure per logical creation (the whole
  retry sequence).
- **Connection created during `close()` could be handed out / leak a slot.** Acquire now
  re-checks the closed state after creating and destroys the orphan.
- **A factory returning a non-distinct object corrupted the checked-out map** (spurious
  `InvalidReleaseError` + leaked slot). Such a connection is now rejected at checkout with
  `ConnectionCreateError`, whether its twin is idle or checked out.
- **Async `acquire()` could leak a just-built connection on cancellation.** If a task was
  cancelled while `_register_new` waited on a contended condition lock, the connection it had
  already created was never closed and its slot was never freed (permanent `size` inflation,
  invisible to leak detection). Acquire now disposes of the orphan; disposal is shielded so a
  second cancellation can't interrupt it, and the original exception propagates unchanged.
- **`on_connection_created` / `PoolStats.created` no longer count a rejected duplicate.**
  The event now fires when a connection is actually adopted, so `created` reflects real
  connections and still balances `closed` (a connection built but destroyed because the pool
  closed mid-create counts as both).

### Added

- **Background idle health checks** (`PoolConfig.validate_idle`, default off): the maintenance
  sweep validates idle connections and reaps dead ones, instead of waiting for the next
  checkout. Each connection is briefly reserved and pinged outside the lock; the idle timer is
  preserved for healthy ones.
- **Create-latency percentiles** in `PoolStats` (`avg/p50/p95/p99_create_latency`), over the same
  bounded recent window as the acquire-wait aggregates.
- **In-use leak detection** (`PoolConfig.leak_detection_timeout`): warns via log and the new
  `PoolListener.on_connection_leak` hook when a borrowed connection is held too long
  (warn-only; never force-reclaimed). Surfaced as `PoolStats.leaks`.
- **Graceful drain**: `close(timeout=...)` waits up to `timeout` for in-use connections to
  return before shutting down.
- **`AsyncPoolRegistry`**: async twin of `PoolRegistry` (one pool per key). Both registries'
  `connection()` now accept a per-call `timeout`.
- **Acquire-wait percentiles** in `PoolStats` (`p50/p95/p99_acquire_wait`), estimated over a
  bounded recent window.
- **Backpressure** (`PoolConfig.max_waiters`): cap callers blocked in `acquire()`; excess
  callers fail fast with the new `PoolExhaustedError`.
- **`ResourceWarning`** when a pool is garbage-collected without `close()`.
- **`CircuitBreaker.record_abort()`**: release a half-open probe slot without counting the
  call as success or failure.

### Changed

- The breaker no longer trips on acquire-timeout / `PoolClosedError` / cancellation during
  creation (those are not backend faults). A real create failure **or** a `create_timeout`
  firing (a genuinely slow backend) counts; an async create that times out only because the
  *acquire* deadline elapsed is now reported as `PoolTimeoutError` and recorded as an abort,
  not a failure.
- `avg_acquire_wait` is now computed over the same recent window as the percentiles, so the
  average and percentiles always describe the same population.
- **Python 3.14 added to the supported range** (CI matrix + classifier); the suite, build,
  and clean-install all pass on it. Supported: 3.11–3.14.
- Documented that with `validate_idle` on, idle connections briefly reserved by the
  background health check count as `in_use` (not `idle`) in `stats()` while being pinged.
