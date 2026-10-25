package io.github.jeffeek.connpool;

import org.junit.jupiter.api.Test;

class ApiSurfaceTest {
  @Test
  void poolListenerBaseMethodsDoNothing() {
    PoolListener l = new PoolListener();
    PooledConnection<Object> c = new PooledConnection<>(new Object(), 0.0);
    l.onConnectionCreated(c, 0.0);
    l.onConnectionClosed(c, CloseReason.IDLE);
    l.onCreateFailed(new RuntimeException());
    l.onAcquired(c, 0.0);
    l.onReleased(c, 0.0);
    l.onAcquireTimeout(0.0);
    l.onConnectionLeak(c, 0.0);
    l.onBreakerStateChange(BreakerState.CLOSED, BreakerState.OPEN);
  }

  @Test
  void threadsHelper() {
    new Threads();
    Threads.join(null); // null thread -> no-op
  }
}
