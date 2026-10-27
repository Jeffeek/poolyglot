# connpool-fuzz (Java)

Jazzer harness for the async primitives — `AsyncAwait.await`'s shield/cancellation race and
`AsyncConnectionPool`'s lock/Condition waiter bookkeeping — driven through `acquireAsync` /
`releaseAsync` with a `CancelToken` cancelled mid-flight. It is a standalone Maven module (not in the
root reactor), so the library's test/coverage gate never builds it.

## Requirements

JDK 17+ and Maven. The library must be installed to the local Maven repo first so the module can
resolve it:

```bash
(cd java && mvn -DskipTests install)
```

## Build & run

```bash
cd java/fuzz
JAZZER_FUZZ=1 mvn test                                       # coverage-guided fuzzing
JAZZER_FUZZ=1 mvn test -Djazzer.fuzz_args="-runs=100000"     # bounded run
```

A finding is any `fail(...)` the harness raises — e.g. a cancelled acquire leaving `idle + inUse != size`,
or an unexpected exception from `acquireAsync` / `releaseAsync`.
