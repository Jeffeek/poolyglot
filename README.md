# poolyglot

![connpool](https://raw.githubusercontent.com/Jeffeek/poolyglot/main/docs/assets/banner.svg)

A battle-tested connection-pool model — slot-reserved sizing, health & rotation rules, a circuit
breaker, backoff retries, and first-class observability — implemented *per language* and kept
API-parallel, so switching stacks is a syntax change, not a rewrite.

[![Python CI](https://github.com/Jeffeek/poolyglot/actions/workflows/python.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/python.yml)
[![.NET CI](https://github.com/Jeffeek/poolyglot/actions/workflows/dotnet.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/dotnet.yml)
[![Java CI](https://github.com/Jeffeek/poolyglot/actions/workflows/java.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/java.yml)
[![Go CI](https://github.com/Jeffeek/poolyglot/actions/workflows/go.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/go.yml)
[![TypeScript CI](https://github.com/Jeffeek/poolyglot/actions/workflows/typescript.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/typescript.yml)
[![Rust CI](https://github.com/Jeffeek/poolyglot/actions/workflows/rust.yml/badge.svg)](https://github.com/Jeffeek/poolyglot/actions/workflows/rust.yml)
[![codecov](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?logo=codecov&label=coverage)](https://app.codecov.io/gh/Jeffeek/poolyglot)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Jeffeek/poolyglot/badge)](https://scorecard.dev/viewer/?uri=github.com/Jeffeek/poolyglot)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/5cd3f7932d1149cf93182ecdc1bd790f)](https://app.codacy.com/gh/Jeffeek/poolyglot/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![license](https://img.shields.io/badge/license-MIT-blue)](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE)

---

Pools **anything** a small user-supplied factory can `create`, `validate`, `reset` and `close` —
DB drivers, HTTP sessions, SSH clients, raw sockets. The pool knows nothing about your connection
except what the factory tells it.

## Why poolyglot

- **Pools anything** — one tiny factory interface; the pool stays backend-agnostic.
- **Identical across languages** — same field names, event names, defaults, and wire values. Learn it once.
- **Sync *and* async** — thread-based and `async`/`await` twins of the same model (TypeScript is async-only — Node has no blocking acquire).
- **Resilient by default** — backoff retries + a circuit breaker that counts *one failure per logical creation*.
- **Observable** — listener hooks plus a stats snapshot with acquire-wait / create-latency percentiles.
- **Zero runtime dependencies** — standard library only (`connpool` ⇒ stdlib, `ConnPool` ⇒ `netstandard2.1`).
- **100% test coverage** — line *and* branch, enforced in CI for every language.

## Languages

| Language | Runtime | Status | Package | Coverage | Source |
| --- | --- | :---: | --- | --- | --- |
| **Python** | sync + `asyncio` | ✅ stable | [![PyPI](https://img.shields.io/pypi/v/connpool?logo=pypi&logoColor=white&label=connpool)](https://pypi.org/project/connpool/) | [![python coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=python&logo=codecov&label=)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=python) | [`python/`](https://github.com/Jeffeek/poolyglot/tree/main/python) |
| **C# / .NET** | sync + `async` | ✅ stable | [![NuGet](https://img.shields.io/nuget/v/ConnPool?logo=nuget&logoColor=white&label=ConnPool)](https://www.nuget.org/packages/ConnPool/) | [![csharp coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=csharp&logo=codecov&label=)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=csharp) | [`csharp/`](https://github.com/Jeffeek/poolyglot/tree/main/csharp) |
| **Java** | blocking + async | ✅ stable | [![Maven Central](https://img.shields.io/maven-central/v/io.github.jeffeek/connpool?logo=apachemaven&logoColor=white&label=connpool)](https://central.sonatype.com/artifact/io.github.jeffeek/connpool) | [![java coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=java&logo=codecov&label=)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=java) | [`java/`](https://github.com/Jeffeek/poolyglot/tree/main/java) |
| **Go** | blocking + `context` | ✅ stable | [![Go Reference](https://pkg.go.dev/badge/github.com/Jeffeek/poolyglot/go.svg)](https://pkg.go.dev/github.com/Jeffeek/poolyglot/go) | [![go coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=go&logo=codecov&label=)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=go) | [`go/`](https://github.com/Jeffeek/poolyglot/tree/main/go) |
| **TypeScript** | `async` / Promises | ✅ stable | [![npm](https://img.shields.io/npm/v/%40jeffeek%2Fconnpool?logo=npm&logoColor=white&label=%40jeffeek%2Fconnpool)](https://www.npmjs.com/package/@jeffeek/connpool) | [![typescript coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=typescript&logo=codecov&label=)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=typescript) | [`typescript/`](https://github.com/Jeffeek/poolyglot/tree/main/typescript) |
| **Rust** | blocking + `tokio` | ✅ stable | [![crates.io](https://img.shields.io/crates/v/connpool-rs?logo=rust&logoColor=white&label=connpool-rs)](https://crates.io/crates/connpool-rs) | [![rust coverage](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?flag=rust&logo=codecov&label=)](https://app.codecov.io/gh/Jeffeek/poolyglot?flags%5B0%5D=rust) | [`rust/`](https://github.com/Jeffeek/poolyglot/tree/main/rust) |

Each language lives in its own top-level directory with its own build and tests, but shares one
`VERSION`, one CHANGELOG per language, and the common behavior spec. CI runs a per-language workflow
scoped to that directory (see [`.github/workflows/`](https://github.com/Jeffeek/poolyglot/tree/main/.github/workflows)).

Full per-language docs:
- **[Python →](https://github.com/Jeffeek/poolyglot/blob/main/python/README.md)**
- **[C# / .NET →](https://github.com/Jeffeek/poolyglot/blob/main/csharp/README.md)**
- **[Java →](https://github.com/Jeffeek/poolyglot/blob/main/java/README.md)**
- **[Go →](https://github.com/Jeffeek/poolyglot/blob/main/go/README.md)**
- **[TypeScript →](https://github.com/Jeffeek/poolyglot/blob/main/typescript/README.md)**
- **[Rust →](https://github.com/Jeffeek/poolyglot/blob/main/rust/README.md)**

## Shared model

Every implementation targets the same contract:

| Area | Behavior |
| --- | --- |
| **Sizing** | `min_size` kept warm, `max_size` a hard cap (a slot is reserved *before* a connection is created). |
| **Rotation / health** | max lifetime (jittered), max uses, idle eviction, validate-on-checkout, optional background idle checks. |
| **Resilience** | exponential-backoff retries + a circuit breaker (one failure per *logical* creation, not per attempt). |
| **Backpressure** | cap the blocked-caller queue; excess callers fail fast. |
| **Observability** | listener hooks + a stats snapshot (counters + acquire-wait / create-latency percentiles). |
| **Shutdown** | idempotent close, graceful drain, no leaked slots on cancellation. |

> Field names, event names, and defaults are **identical** across languages — switching is a syntax
> change, not a mental-model change.

## When to use (and when not to)

**Use a connection pool when:**

- Creating connections is expensive (≥10ms network handshake, TLS negotiation, auth)
- You make many requests over a connection's lifetime (reuse justifies overhead)
- Backends impose connection limits (DB `max_connections`, API rate limits)
- You need connection health checks or automatic retries

**Don't pool when:**

- Connection creation is cheap (<1ms, e.g., in-memory queues, local pipes)
- Connections are single-use (can't be reused, no keep-alive)
- You have ≤1 concurrent request (no contention, just create-and-close)
- Serverless / short-lived functions (pool setup cost > benefit)

**Use multiple pools (`PoolRegistry`) when:**

- **Per-host HTTP keep-alive** — avoid head-of-line blocking across hosts
- **Multi-tenant isolation** — quota enforcement, separate limits per tenant
- **Priority bulkheading** — interactive vs batch with different `max_size`
- **Regional backends** — one pool per datacenter/availability zone

**Common anti-patterns:**

- **One global pool for all backends** → cross-backend contention; use `PoolRegistry` keyed by host
- **Pool in a serverless handler** → connection setup overhead > function lifetime; prefer persistent connections from the runtime or SDK
- **Undersized `max_size` for concurrency** → if 16 threads but `max_size=4`, 12 threads always wait; set `max_size` ≥ peak concurrency
- **No `acquire_timeout`** → infinite wait masks availability issues; set finite timeout and fail fast
- **Pooling stateful connections without `reset()`** → session bleed between requests; implement `factory.reset()`

## Architecture

The library is layered with a strict **downward-only** dependency direction — each layer may only
use the ones below it (`core → resilience → policy → factories/observability → pools`), and the same
shape holds in every language. Sync and async pools are parallel implementations of one model.

📐 **[docs/architecture.md](https://github.com/Jeffeek/poolyglot/blob/main/docs/architecture.md)** has the diagrams — layering, connection lifecycle,
and circuit-breaker states.

## Quality bar

- **100% line + branch coverage**, gated in CI for each language (`run_tests.sh`).
- **Static analysis in CI** — each language has a `lint` job and a `lint.sh` (ruff / Roslyn analyzers /
  SpotBugs / golangci-lint / Biome / clippy). The gate blocks on bug & dead-code rules; formatting is
  warn-only. **CodeQL** scans all six languages.
- Sync and async are **parallel implementations** of one model, with mirrored test suites.
- Releases are cut from the root `VERSION` and published via **trusted publishing** (OIDC, no stored
  keys) to PyPI and NuGet, with built artifacts attached to each GitHub Release.

## Contributing

Work happens inside the relevant language directory. A behavioral change to one language almost
always needs the mirror change in the others (and in the "Shared model" section above) — keeping the
implementations in lock-step is the whole point.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
