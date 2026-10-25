/**
 * One pool per key (e.g. per host, per tenant, per database) created on demand.
 *
 * The registry is *lazy*: a pool is built on first use and is **not** `open()`-ed, so it does no
 * background maintenance unless you open it yourself (`registry.pool(key).open()`).
 */
import type { AsyncConnectionFactory } from "../factories/async-factory.js";
import type { PoolConfig } from "../policy/config.js";
import {
  type AcquireOptions,
  AsyncConnectionPool,
  type AsyncConnectionPoolOptions,
} from "./async-pool.js";

export class AsyncPoolRegistry<K, T extends object> {
  private readonly pools = new Map<K, AsyncConnectionPool<T>>();
  private _closed = false;

  constructor(
    private readonly factoryFor: (key: K) => AsyncConnectionFactory<T>,
    private readonly config?: PoolConfig,
    private readonly poolOptions: AsyncConnectionPoolOptions = {},
  ) {}

  pool(key: K): AsyncConnectionPool<T> {
    if (this._closed) {
      throw new Error("registry is closed");
    }
    let pool = this.pools.get(key);
    if (pool === undefined) {
      pool = new AsyncConnectionPool(this.factoryFor(key), this.config, {
        ...this.poolOptions,
        name: String(key),
      });
      this.pools.set(key, pool);
    }
    return pool;
  }

  withConnection<R>(
    key: K,
    fn: (conn: T) => R | Promise<R>,
    options: AcquireOptions = {},
  ): Promise<R> {
    return this.pool(key).withConnection(fn, options);
  }

  async close(): Promise<void> {
    this._closed = true;
    const pools = [...this.pools.values()];
    this.pools.clear();
    for (const pool of pools) {
      await pool.close();
    }
  }

  get closed(): boolean {
    return this._closed;
  }
}
