# poolyglot

One connection pool, many languages — the same battle-tested pooling model
(slot-reserved sizing, health/rotation rules, circuit breaker, retries,
observability) implemented per language and kept API-parallel across them.

Pools *anything* a small user-supplied factory can create, validate, reset and
close: DB drivers, HTTP sessions, SSH clients, sockets.

## Languages

| Language | Status | Directory | Package |
| --- | --- | --- | --- |
| Python (sync + asyncio) | ✅ stable | [`python/`](python/) | `connpool` |
| C# / .NET (sync + async) | ✅ stable | [`csharp/`](csharp/) | `ConnPool` |
| Java | 🚧 planned | `java/` | — |
| Go | 🚧 planned | `go/` | — |
| Rust | 🚧 planned | `rust/` | — |
| TypeScript (Node) | 🚧 planned | `typescript/` | — |

Each language lives in its own top-level directory with its own build and tests,
but shares one version, one CHANGELOG, and the common behavior spec. CI runs a
per-language workflow scoped to that directory (see `.github/workflows/`).

## Shared model

Every implementation targets the same contract:

- **Sizing** — `min_size` kept warm, `max_size` a hard cap (slot reserved before create).
- **Rotation / health** — max lifetime (jittered), max uses, idle eviction, validate-on-checkout, optional background idle checks.
- **Resilience** — exponential-backoff retries + a circuit breaker (one failure per logical creation, not per attempt).
- **Backpressure** — cap blocked callers; excess fail fast.
- **Observability** — listener hooks + a stats snapshot (counters + acquire-wait / create-latency percentiles).
- **Shutdown** — idempotent close, graceful drain, no leaked slots on cancellation.

Field names, event names, and defaults are kept identical across languages so
switching is a syntax change, not a mental-model change.

## License

MIT — see [LICENSE](LICENSE).
