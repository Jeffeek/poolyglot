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
[![codecov](https://img.shields.io/codecov/c/github/Jeffeek/poolyglot?logo=codecov&label=coverage)](https://app.codecov.io/gh/Jeffeek/poolyglot)
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
| Rust | — | 🚧 planned | — | — | `rust/` |

Each language lives in its own top-level directory with its own build and tests, but shares one
`VERSION`, one CHANGELOG per language, and the common behavior spec. CI runs a per-language workflow
scoped to that directory (see [`.github/workflows/`](https://github.com/Jeffeek/poolyglot/tree/main/.github/workflows)).

Full per-language docs:
- **[Python →](https://github.com/Jeffeek/poolyglot/blob/main/python/README.md)**
- **[C# / .NET →](https://github.com/Jeffeek/poolyglot/blob/main/csharp/README.md)**
- **[Java →](https://github.com/Jeffeek/poolyglot/blob/main/java/README.md)**
- **[Go →](https://github.com/Jeffeek/poolyglot/blob/main/go/README.md)**
- **[TypeScript →](https://github.com/Jeffeek/poolyglot/blob/main/typescript/README.md)**

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

## Architecture

The library is layered with a strict **downward-only** dependency direction — each layer may only
use the ones below it (`core → resilience → policy → factories/observability → pools`), and the same
shape holds in every language. Sync and async pools are parallel implementations of one model.

📐 **[docs/architecture.md](https://github.com/Jeffeek/poolyglot/blob/main/docs/architecture.md)** has the diagrams — layering, connection lifecycle,
and circuit-breaker states.

## Quality bar

- **100% line + branch coverage**, gated in CI for each language (`run_tests.sh`).
- Sync and async are **parallel implementations** of one model, with mirrored test suites.
- Releases are cut from the root `VERSION` and published via **trusted publishing** (OIDC, no stored
  keys) to PyPI and NuGet, with built artifacts attached to each GitHub Release.

## Contributing

Work happens inside the relevant language directory. A behavioral change to one language almost
always needs the mirror change in the others (and in the "Shared model" section above) — keeping the
implementations in lock-step is the whole point. See [`CLAUDE.md`](https://github.com/Jeffeek/poolyglot/blob/main/CLAUDE.md) for the repo contract.

## License

MIT — see [LICENSE](https://github.com/Jeffeek/poolyglot/blob/main/LICENSE).
