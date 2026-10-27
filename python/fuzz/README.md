# connpool fuzz (Python)

Atheris (libFuzzer) harness for the async pool's primitives — `asyncio.Condition` waiter
coordination, `asyncio.shield`-guarded disposal, and `task.cancel()` cancellation — driven through
`AsyncConnectionPool`. It lives outside `src/` and `tests/`, so it never affects the 100% coverage
gate and the library stays standard-library-only. The deterministic twin that runs in CI is
`tests/pools/test_async_pool_property.py`.

## Requirements

Atheris needs clang/libFuzzer (Linux; it is not importable on Windows):

```bash
pip install atheris
```

## Build & run

```bash
python fuzz/fuzz_async_pool.py -max_total_time=60
```

A crash is any unhandled exception the harness doesn't treat as an expected pool error, or a broken
invariant (`idle + in_use != size`, or a non-empty waiter set at rest) — a cancelled acquire that
leaked a reserved slot.
