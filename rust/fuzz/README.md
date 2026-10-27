# connpool-fuzz

`cargo-fuzz` (libFuzzer) harness for the async primitives in `src/pools/async_pool.rs` —
`tokio::sync::Notify` waiter coordination and the Drop-guard cancellation path (`SlotGuard` /
`BorrowGuard`) — driven through `AsyncConnectionPool::acquire` / `release` by dropping an in-flight
future mid-poll. It is a separate crate (not a workspace member), so `cargo test` / `clippy` /
coverage from `rust/` never build it and the published library stays zero-runtime-deps.

## Requirements

A nightly toolchain and cargo-fuzz, which need clang/LLVM (Linux or macOS):

```bash
rustup toolchain install nightly
cargo install cargo-fuzz
```

## Build & run

```bash
cd rust/fuzz
cargo +nightly fuzz run async_pool_cancel -- -max_total_time=60
cargo +nightly fuzz build        # build only, no run
```

A crash is any panic the harness doesn't treat as an expected pool error, or its `idle + in_use == size`
assertion failing — a dropped future that leaked a reserved slot or a briefly-borrowed connection.
