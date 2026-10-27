# ConnPool.Fuzz

SharpFuzz (libFuzzer) harness for the async primitives in `src/ConnPool/Pools/AsyncPrimitives.cs`
— `AsyncCondition` and `AwaitShielded` — driven through `AsyncConnectionPool<T>.AcquireAsync` /
`ReleaseAsync` with a `CancellationToken` cancelled mid-acquire. It lives outside `ConnPool.sln`, so
the test/coverage gate never builds it and `src/ConnPool` stays dependency-free.

## Requirements

A libFuzzer toolchain (clang) — i.e. Linux or WSL — plus the SharpFuzz CLI:

```bash
dotnet tool install --global SharpFuzz.CommandLine
```

`Fuzzer.LibFuzzer.Run` does not run libFuzzer itself; it talks to the native `libfuzzer-dotnet`
loader. Build that loader once (its source is tiny and stable):

```bash
curl -fsSL -o libfuzzer-dotnet.cc \
  https://raw.githubusercontent.com/Metalnem/libfuzzer-dotnet/master/libfuzzer-dotnet.cc
clang -fsanitize=fuzzer libfuzzer-dotnet.cc -o libfuzzer-dotnet
```

## Build & run

```bash
dotnet build fuzz/ConnPool.Fuzz/ConnPool.Fuzz.csproj -c Release
sharpfuzz fuzz/ConnPool.Fuzz/bin/Release/net8.0/ConnPool.dll          # instrument the library
mkdir -p fuzz/corpus
./libfuzzer-dotnet \
  --target_path="$(command -v dotnet)" \
  --target_arg="$PWD/fuzz/ConnPool.Fuzz/bin/Release/net8.0/ConnPool.Fuzz.dll" \
  fuzz/corpus -max_total_time=60
```

A crash is any unhandled exception other than `OperationCanceledException`, `PoolTimeoutError`, or
`InvalidReleaseError` (the expected outcomes of racing cancellation against acquire/release), or the
harness's own `idle + in_use != size` assertion — a cancelled `AcquireAsync` that leaked a reserved slot.
