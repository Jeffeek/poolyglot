package io.github.jeffeek.connpool;

import static io.github.jeffeek.connpool.TestSupport.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class FactoryTest {
  static final class Conn implements AutoCloseable {
    boolean closed;

    @Override
    public void close() {
      closed = true;
    }
  }

  static final class Minimal extends ConnectionFactory<Conn> {
    @Override
    public Conn create() {
      return new Conn();
    }
  }

  static final class MinimalObject extends ConnectionFactory<Object> {
    @Override
    public Object create() {
      return new Object();
    }
  }

  @Test
  void baseClassDefaults() {
    Minimal f = new Minimal();
    Conn c = f.create();
    assertTrue(f.validate(c));
    assertFalse(f.isFatalError(new IllegalStateException()));
    f.reset(c);
    f.close(c);
    assertTrue(c.closed);
  }

  @Test
  void baseCloseWithoutAutoCloseable() {
    new MinimalObject().close(new Object()); // not AutoCloseable: no-op branch
  }

  @Test
  void callableFactoryAllCallbacks() {
    List<Conn> resets = new ArrayList<>();
    CallableFactory<Conn> f =
        CallableFactory.<Conn>builder(Conn::new)
            .validate(c -> true)
            .reset(resets::add)
            .isFatalError(e -> e instanceof IllegalArgumentException)
            .build();
    Conn c = f.create();
    assertTrue(f.validate(c));
    f.reset(c);
    assertEquals(List.of(c), resets);
    assertTrue(f.isFatalError(new IllegalArgumentException()));
    assertFalse(f.isFatalError(new IllegalStateException()));
  }

  @Test
  void callableFactoryDefaultsWhenCallbacksOmitted() {
    CallableFactory<Conn> f = CallableFactory.<Conn>builder(Conn::new).build();
    Conn c = f.create();
    assertTrue(f.validate(c));
    f.reset(c);
    assertFalse(f.isFatalError(new IllegalStateException()));
    f.close(c); // falls back to AutoCloseable
    assertTrue(c.closed);
  }

  @Test
  void callableFactoryValidateReturnsFalse() {
    CallableFactory<Conn> f = CallableFactory.<Conn>builder(Conn::new).validate(c -> false).build();
    assertFalse(f.validate(new Conn()));
  }

  @Test
  void callableFactoryExplicitClose() {
    List<Conn> closed = new ArrayList<>();
    CallableFactory<Conn> f = CallableFactory.<Conn>builder(Conn::new).close(closed::add).build();
    Conn c = f.create();
    f.close(c);
    assertEquals(List.of(c), closed);
  }

  @Test
  void asyncBaseClassDefaults() {
    AsyncConnectionFactory<Conn> f =
        new AsyncConnectionFactory<>() {
          @Override
          public java.util.concurrent.CompletableFuture<Conn> createAsync(CancelToken token) {
            return java.util.concurrent.CompletableFuture.completedFuture(new Conn());
          }
        };
    Conn c = AsyncAwait.join(f.createAsync(CancelToken.NONE));
    assertTrue(AsyncAwait.join(f.validateAsync(c, CancelToken.NONE)));
    AsyncAwait.join(f.resetAsync(c, CancelToken.NONE));
    assertFalse(f.isFatalError(new IllegalStateException()));
    AsyncAwait.join(f.closeAsync(c, CancelToken.NONE));
    assertTrue(c.closed);
  }

  @Test
  void asyncBaseCloseWithoutAutoCloseable() {
    AsyncConnectionFactory<Object> f =
        new AsyncConnectionFactory<>() {
          @Override
          public java.util.concurrent.CompletableFuture<Object> createAsync(CancelToken token) {
            return java.util.concurrent.CompletableFuture.completedFuture(new Object());
          }
        };
    AsyncAwait.join(f.closeAsync(new Object(), CancelToken.NONE));
  }

  static final class BadCloseable implements AutoCloseable {
    @Override
    public void close() throws Exception {
      throw new Exception("boom");
    }
  }

  @Test
  void syncDefaultCloseWrapsCheckedException() {
    ConnectionFactory<BadCloseable> f =
        new ConnectionFactory<>() {
          @Override
          public BadCloseable create() {
            return new BadCloseable();
          }
        };
    assertThrowsExactly(RuntimeException.class, () -> f.close(new BadCloseable()));
  }

  @Test
  void asyncDefaultCloseReturnsFailedFuture() {
    AsyncConnectionFactory<BadCloseable> f =
        new AsyncConnectionFactory<>() {
          @Override
          public java.util.concurrent.CompletableFuture<BadCloseable> createAsync(
              CancelToken token) {
            return java.util.concurrent.CompletableFuture.completedFuture(new BadCloseable());
          }
        };
    assertThrowsExactly(
        java.util.concurrent.CompletionException.class,
        () -> AsyncAwait.join(f.closeAsync(new BadCloseable(), CancelToken.NONE)));
  }

  @Test
  void asyncCallableFactoryAllCallbacks() {
    List<Conn> resets = new ArrayList<>();
    AsyncCallableFactory<Conn> f =
        AsyncCallableFactory.<Conn>builder(
                t -> java.util.concurrent.CompletableFuture.completedFuture(new Conn()))
            .validate((c, t) -> java.util.concurrent.CompletableFuture.completedFuture(true))
            .reset(
                (c, t) -> {
                  resets.add(c);
                  return java.util.concurrent.CompletableFuture.completedFuture(null);
                })
            .isFatalError(e -> e instanceof IllegalArgumentException)
            .build();
    Conn c = AsyncAwait.join(f.createAsync(CancelToken.NONE));
    assertTrue(AsyncAwait.join(f.validateAsync(c, CancelToken.NONE)));
    AsyncAwait.join(f.resetAsync(c, CancelToken.NONE));
    assertEquals(List.of(c), resets);
    assertTrue(f.isFatalError(new IllegalArgumentException()));
    assertFalse(f.isFatalError(new IllegalStateException()));
  }

  @Test
  void asyncCallableFactoryDefaultsWhenCallbacksOmitted() {
    AsyncCallableFactory<Conn> f =
        AsyncCallableFactory.<Conn>builder(
                t -> java.util.concurrent.CompletableFuture.completedFuture(new Conn()))
            .build();
    Conn c = AsyncAwait.join(f.createAsync(CancelToken.NONE));
    assertTrue(AsyncAwait.join(f.validateAsync(c, CancelToken.NONE)));
    AsyncAwait.join(f.resetAsync(c, CancelToken.NONE));
    assertFalse(f.isFatalError(new IllegalStateException()));
    AsyncAwait.join(f.closeAsync(c, CancelToken.NONE));
    assertTrue(c.closed);
  }

  @Test
  void fromSyncAdaptsSyncCallbacks() {
    List<Conn> resets = new ArrayList<>();
    List<Conn> closed = new ArrayList<>();
    AsyncCallableFactory<Conn> f =
        AsyncCallableFactory.fromSync(Conn::new)
            .closeSync(closed::add)
            .validateSync(c -> true)
            .resetSync(resets::add)
            .isFatalError(e -> e instanceof IllegalArgumentException)
            .build();
    Conn c = AsyncAwait.join(f.createAsync(CancelToken.NONE));
    assertTrue(AsyncAwait.join(f.validateAsync(c, CancelToken.NONE)));
    AsyncAwait.join(f.resetAsync(c, CancelToken.NONE));
    AsyncAwait.join(f.closeAsync(c, CancelToken.NONE));
    assertEquals(List.of(c), resets);
    assertEquals(List.of(c), closed);
    assertTrue(f.isFatalError(new IllegalArgumentException()));
  }

  @Test
  void fromSyncOmittingCallbacksUsesDefaults() {
    AsyncCallableFactory<Conn> f = AsyncCallableFactory.fromSync(Conn::new).build();
    Conn c = AsyncAwait.join(f.createAsync(CancelToken.NONE));
    assertTrue(AsyncAwait.join(f.validateAsync(c, CancelToken.NONE)));
    AsyncAwait.join(f.resetAsync(c, CancelToken.NONE));
    AsyncAwait.join(f.closeAsync(c, CancelToken.NONE));
    assertTrue(c.closed);
  }
}
