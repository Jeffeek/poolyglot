package io.github.jeffeek.connpool;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** Fan a hook out to every listener; a listener that throws is logged and skipped. */
final class ListenerGroup {
  private static final Logger LOG = System.getLogger("connpool");

  private final List<PoolListener> listeners;

  ListenerGroup(List<PoolListener> listeners) {
    this.listeners = new ArrayList<>(listeners);
  }

  void dispatch(Consumer<PoolListener> hook) {
    for (PoolListener listener : listeners) {
      try {
        hook.accept(listener);
      } catch (RuntimeException ex) {
        // listeners must never break the pool
        LOG.log(Level.ERROR, "connpool: pool listener " + listener + " failed", ex);
      }
    }
  }
}
