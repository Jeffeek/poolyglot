package io.github.jeffeek.connpool;

import static io.github.jeffeek.connpool.TestSupport.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.Test;

class RegistryTest {
  static final class KeyedConn {
    final String key;

    KeyedConn(String key) {
      this.key = key;
    }
  }

  /** A key whose toString() is null, exercising the "pool" fallback name. */
  static final class NullNameKey {
    @Override
    public String toString() {
      return null;
    }
  }

  private static <X> X await(CompletableFuture<X> f) {
    try {
      return f.get(10, TimeUnit.SECONDS);
    } catch (ExecutionException e) {
      Throwable c = e.getCause();
      if (c instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      throw new RuntimeException(c);
    } catch (InterruptedException | TimeoutException e) {
      throw new RuntimeException(e);
    }
  }

  // ----------------------------------------------------------------- sync
  @Test
  void syncOnePoolPerKey() {
    List<String> made = new ArrayList<>();
    PoolRegistry<String, KeyedConn> reg =
        new PoolRegistry<>(
            key -> {
              made.add(key);
              return CallableFactory.<KeyedConn>builder(() -> new KeyedConn(key)).build();
            },
            TestSupport.quietConfig());

    KeyedConn a1 = reg.use("a", c -> c, Timeout.unset());
    KeyedConn a2 = reg.use("a", c -> c, Timeout.unset());
    KeyedConn b = reg.use("b", c -> c, Timeout.unset());
    assertSame(a1, a2);
    made.sort(String::compareTo);
    assertEquals(List.of("a", "b"), made);
    assertEquals("b", b.key);

    reg.close();
    assertThrowsExactly(IllegalStateException.class, () -> reg.pool("a"));
  }

  @Test
  void syncAcquireUseVoidConnectionAndClose() {
    try (PoolRegistry<String, KeyedConn> reg =
        new PoolRegistry<>(
            k -> CallableFactory.<KeyedConn>builder(() -> new KeyedConn(k)).build(),
            TestSupport.quietConfig())) {
      KeyedConn direct = reg.acquire("a", Timeout.unset());
      reg.pool("a").release(direct);

      boolean[] ran = {false};
      reg.use("a", (java.util.function.Consumer<KeyedConn>) c -> ran[0] = true, Timeout.unset());
      assertTrue(ran[0]);

      try (ConnectionPool<KeyedConn>.Lease lease = reg.connection("b", Timeout.unset())) {
        assertEquals("b", lease.get().key);
      }
      reg.close();
    }
  }

  @Test
  void syncKeyWithNullToStringFallsBackToPoolName() {
    try (PoolRegistry<NullNameKey, KeyedConn> reg =
        new PoolRegistry<>(
            k -> CallableFactory.<KeyedConn>builder(() -> new KeyedConn("x")).build(),
            TestSupport.quietConfig())) {
      ConnectionPool<KeyedConn> pool = reg.pool(new NullNameKey());
      assertEquals("pool", pool.name());
    }
  }

  @Test
  void syncDefaultConfigConstructor() {
    try (PoolRegistry<String, KeyedConn> reg =
        new PoolRegistry<>(
            k -> CallableFactory.<KeyedConn>builder(() -> new KeyedConn(k)).build())) {
      KeyedConn c = reg.acquire("a", Timeout.unset());
      reg.pool("a").release(c);
    }
  }

  // ----------------------------------------------------------------- async
  @Test
  void asyncOnePoolPerKey() {
    List<String> made = new ArrayList<>();
    AsyncPoolRegistry<String, KeyedConn> reg =
        new AsyncPoolRegistry<>(
            key -> {
              made.add(key);
              return AsyncCallableFactory.<KeyedConn>builder(
                      t -> CompletableFuture.completedFuture(new KeyedConn(key)))
                  .build();
            },
            TestSupport.quietConfig());

    KeyedConn a1 = await(reg.acquireAsync("a", Timeout.unset(), CancelToken.NONE));
    await(reg.pool("a").releaseAsync(a1));
    KeyedConn a2 = await(reg.acquireAsync("a", Timeout.unset(), CancelToken.NONE));
    KeyedConn b = await(reg.acquireAsync("b", Timeout.unset(), CancelToken.NONE));
    assertSame(a1, a2);
    made.sort(String::compareTo);
    assertEquals(List.of("a", "b"), made);
    assertEquals("b", b.key);

    await(reg.closeAsync());
    assertThrowsExactly(IllegalStateException.class, () -> reg.pool("a"));
  }

  @Test
  void asyncUseVoidAndClose() {
    AsyncPoolRegistry<String, KeyedConn> reg =
        new AsyncPoolRegistry<>(
            k ->
                AsyncCallableFactory.<KeyedConn>builder(
                        t -> CompletableFuture.completedFuture(new KeyedConn(k)))
                    .build(),
            TestSupport.quietConfig());

    boolean[] ran = {false};
    await(
        reg.useAsync(
            "a",
            c -> {
              ran[0] = true;
              return CompletableFuture.completedFuture(null);
            },
            Timeout.unset(),
            CancelToken.NONE));
    assertTrue(ran[0]);

    await(reg.closeAsync());
    assertThrowsExactly(IllegalStateException.class, () -> reg.pool("a"));
  }

  @Test
  void asyncKeyWithNullToStringFallsBackToPoolName() {
    AsyncPoolRegistry<NullNameKey, KeyedConn> reg =
        new AsyncPoolRegistry<>(
            k ->
                AsyncCallableFactory.<KeyedConn>builder(
                        t -> CompletableFuture.completedFuture(new KeyedConn("x")))
                    .build(),
            TestSupport.quietConfig());
    AsyncConnectionPool<KeyedConn> pool = reg.pool(new NullNameKey());
    assertEquals("pool", pool.name());
    await(reg.closeAsync());
  }

  @Test
  void asyncDefaultConfigConstructor() {
    AsyncPoolRegistry<String, KeyedConn> reg =
        new AsyncPoolRegistry<>(
            k ->
                AsyncCallableFactory.<KeyedConn>builder(
                        t -> CompletableFuture.completedFuture(new KeyedConn(k)))
                    .build());
    KeyedConn c = await(reg.acquireAsync("a", Timeout.unset(), CancelToken.NONE));
    await(reg.pool("a").releaseAsync(c));
    await(reg.closeAsync());
  }
}
