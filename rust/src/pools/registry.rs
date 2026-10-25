//! `PoolRegistry`: one [`ConnectionPool`] per key (per host, tenant, database…), created on
//! demand.

use std::collections::HashMap;
use std::fmt::Display;
use std::hash::Hash;
use std::sync::{Arc, Mutex};

use crate::core::clock::Clock;
use crate::core::errors::PoolError;
use crate::core::timeouts::Timeout;
use crate::factories::sync_factory::ConnectionFactory;
use crate::observability::events::PoolListener;
use crate::policy::config::PoolConfig;
use crate::pools::sync_pool::{ConnectionPool, PoolOptions};

type FactoryFor<K, T> = Box<dyn Fn(&K) -> Box<dyn ConnectionFactory<T>> + Send + Sync>;

struct RegState<K, T> {
    pools: HashMap<K, ConnectionPool<T>>,
    closed: bool,
}

/// Hands out one [`ConnectionPool`] per key, built on first use. It is **lazy**: a pool is
/// created on first access and is NOT opened, so it does no background maintenance unless you
/// open it yourself.
pub struct PoolRegistry<K, T> {
    factory_for: FactoryFor<K, T>,
    config: PoolConfig,
    clock: Option<Arc<dyn Clock>>,
    listeners: Vec<Arc<dyn PoolListener<T>>>,
    inner: Mutex<RegState<K, T>>,
}

impl<K, T> PoolRegistry<K, T>
where
    K: Clone + Eq + Hash + Display,
    T: Clone + Eq + Hash + Send + Sync + 'static,
{
    /// Builds a registry. `factory_for` makes the factory for a given key; `config` is shared by
    /// every pool.
    pub fn new<F>(factory_for: F, config: PoolConfig) -> Self
    where
        F: Fn(&K) -> Box<dyn ConnectionFactory<T>> + Send + Sync + 'static,
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

    /// Returns the pool for `key`, creating it on first use. The returned pool is a cheap handle
    /// to the registry-owned pool.
    pub fn pool(&self, key: &K) -> Result<ConnectionPool<T>, PoolError> {
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
        let pool = ConnectionPool::new((self.factory_for)(key), self.config.clone(), Some(options))?;
        g.pools.insert(key.clone(), pool.clone());
        Ok(pool)
    }

    /// Borrows a connection from the pool for `key` for the duration of `f`.
    pub fn with_connection<R>(
        &self,
        key: &K,
        timeout: Timeout,
        f: impl FnOnce(&T) -> R,
    ) -> Result<R, PoolError> {
        self.pool(key)?.with_connection(timeout, f)
    }

    /// Closes every pool and rejects further use.
    pub fn close(&self) {
        let pools = {
            let mut g = self.inner.lock().unwrap();
            g.closed = true;
            std::mem::take(&mut g.pools)
        };
        for (_key, pool) in pools {
            pool.close(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;

    fn registry(h: &Harness) -> PoolRegistry<String, Conn> {
        let fac = h.fac.clone();
        let rec: Arc<dyn PoolListener<Conn>> = Recorder::new();
        PoolRegistry::new(
            move |_key: &String| Box::new(TestFactory(fac.clone())) as Box<dyn ConnectionFactory<Conn>>,
            PoolConfig {
                maintenance_interval: None,
                breaker: None,
                ..Default::default()
            },
        )
        .with_clock(h.clock.clone())
        .with_listeners(vec![rec])
    }

    #[test]
    fn pool_is_created_once_per_key() {
        let h = Harness::new();
        let reg = registry(&h);
        let p1 = reg.pool(&"a".to_string()).unwrap();
        let p2 = reg.pool(&"a".to_string()).unwrap();
        assert_eq!(p1.name(), "a");
        assert_eq!(p2.name(), "a");
        let _b = reg.pool(&"b".to_string()).unwrap();
        // with_connection routes through the per-key pool.
        let id = reg
            .with_connection(&"a".to_string(), Timeout::Unset, |c| c.id)
            .unwrap();
        assert!(id >= 1);
    }

    #[test]
    fn close_rejects_further_use() {
        let h = Harness::new();
        let reg = registry(&h);
        let p = reg.pool(&"a".to_string()).unwrap();
        reg.close();
        assert!(p.is_closed());
        assert!(matches!(
            reg.pool(&"a".to_string()),
            Err(PoolError::Closed(_))
        ));
    }
}
