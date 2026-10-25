//! `AsyncPoolRegistry`: one [`AsyncConnectionPool`] per key, created on demand (feature `tokio`).

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::core::clock::Clock;
use crate::core::errors::PoolError;
use crate::core::timeouts::Timeout;
use crate::factories::async_factory::AsyncConnectionFactory;
use crate::observability::events::PoolListener;
use crate::policy::config::PoolConfig;
use crate::pools::async_pool::AsyncConnectionPool;
use crate::pools::sync_pool::PoolOptions;

type FactoryFor<K, F> = Box<dyn Fn(&K) -> F + Send + Sync>;

struct RegState<K, T, F> {
    pools: HashMap<K, AsyncConnectionPool<T, F>>,
    closed: bool,
}

/// Hands out one [`AsyncConnectionPool`] per key, built on first use. Lazy: pools are not opened
/// automatically.
pub struct AsyncPoolRegistry<K, T, F> {
    factory_for: FactoryFor<K, F>,
    config: PoolConfig,
    clock: Option<Arc<dyn Clock>>,
    listeners: Vec<Arc<dyn PoolListener<T>>>,
    inner: Mutex<RegState<K, T, F>>,
}

impl<K, T, F> AsyncPoolRegistry<K, T, F>
where
    K: Clone + Eq + Hash + Display,
    T: Clone + Eq + Hash + Send + Sync + 'static,
    F: AsyncConnectionFactory<T> + 'static,
{
    /// Builds a registry. `factory_for` makes the factory for a given key; `config` is shared.
    pub fn new<Fun>(factory_for: Fun, config: PoolConfig) -> Self
    where
        Fun: Fn(&K) -> F + Send + Sync + 'static,
    {
        Self {
            factory_for: Box::new(factory_for),
            config,
            clock: None,
            listeners: Vec::new(),
            inner: Mutex::new(RegState {
                pools: HashMap::new(),
                closed: false,
            }),
        }
    }

    /// Shares `clock` with every pool the registry creates.
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Shares `listeners` with every pool the registry creates.
    pub fn with_listeners(mut self, listeners: Vec<Arc<dyn PoolListener<T>>>) -> Self {
        self.listeners = listeners;
        self
    }

    /// Returns the pool for `key`, creating it on first use.
    pub fn pool(&self, key: &K) -> Result<AsyncConnectionPool<T, F>, PoolError> {
        let mut g = self.inner.lock().unwrap();
        if g.closed {
            return Err(PoolError::Closed("registry is closed".to_string()));
        }
        if let Some(pool) = g.pools.get(key) {
            return Ok(pool.clone());
        }
        let options = PoolOptions {
            name: Some(key.to_string()),
            clock: self.clock.clone(),
            rng: None,
            listeners: self.listeners.clone(),
        };
        let pool =
            AsyncConnectionPool::new((self.factory_for)(key), self.config.clone(), Some(options))?;
        g.pools.insert(key.clone(), pool.clone());
        Ok(pool)
    }

    /// Closes every pool and rejects further use.
    pub async fn close(&self) {
        let pools = {
            let mut g = self.inner.lock().unwrap();
            g.closed = true;
            std::mem::take(&mut g.pools)
        };
        for (_key, pool) in pools {
            pool.close(None).await;
        }
    }

    /// Borrows a connection from the pool for `key` for the duration of `f`.
    pub async fn with_connection<R>(
        &self,
        key: &K,
        timeout: Timeout,
        f: impl FnOnce(&T) -> R,
    ) -> Result<R, PoolError> {
        let pool = self.pool(key)?;
        let conn = pool.acquire(timeout).await?;
        let out = f(&conn);
        pool.release(conn).await?;
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_test_support::*;

    fn registry(h: &AsyncHarness) -> AsyncPoolRegistry<String, Conn, AsyncTestFactory> {
        let fac = h.fac.clone();
        let rec: Arc<dyn PoolListener<Conn>> = Recorder::new();
        AsyncPoolRegistry::new(
            move |_key: &String| AsyncTestFactory(fac.clone()),
            PoolConfig {
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
        )
        .with_clock(h.clock.clone())
        .with_listeners(vec![rec])
    }

    #[tokio::test]
    async fn pool_per_key_and_with_connection() {
        let h = AsyncHarness::new();
        let reg = registry(&h);
        let a1 = reg.pool(&"a".to_string()).unwrap();
        let a2 = reg.pool(&"a".to_string()).unwrap();
        assert_eq!(a1.name(), "a");
        assert_eq!(a2.name(), "a");
        let _b = reg.pool(&"b".to_string()).unwrap();
        let id = reg
            .with_connection(&"a".to_string(), Timeout::Unset, |c: &Conn| c.id)
            .await
            .unwrap();
        assert!(id >= 1);
    }

    #[tokio::test]
    async fn close_rejects_further_use() {
        let h = AsyncHarness::new();
        let reg = registry(&h);
        let p = reg.pool(&"a".to_string()).unwrap();
        reg.close().await;
        assert!(p.is_closed());
        assert!(matches!(
            reg.pool(&"a".to_string()),
            Err(PoolError::Closed(_))
        ));
        // with_connection on a closed registry surfaces the error too.
        assert!(
            reg.with_connection(&"a".to_string(), Timeout::Unset, |_c: &Conn| ())
                .await
                .is_err()
        );
    }
}
