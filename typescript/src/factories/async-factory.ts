/** Asynchronous factory contract: the seam users implement to pool anything. */

/** Anything with a callable `close` member (used by the default {@link AsyncConnectionFactory.close}). */
interface MaybeClosable {
  close?: () => unknown;
}

/**
 * The seam you implement to pool anything. `create` is required; the rest have sensible
 * defaults. Every method may be sync or async — the pool awaits the result either way.
 */
export abstract class AsyncConnectionFactory<T> {
  /** Build a brand-new connection. Must return a **distinct** object on every call. */
  abstract create(): T | Promise<T>;

  /** Is `conn` still usable? Default: assume healthy. */
  validate(_conn: T): boolean | Promise<boolean> {
    return true;
  }

  /** Reset per-checkout state before the connection goes back to the pool. Default: no-op. */
  reset(_conn: T): void | Promise<void> {
    return undefined;
  }

  /** Dispose of a connection. Default: call `conn.close()` if it has one. */
  async close(conn: T): Promise<void> {
    const closer = (conn as MaybeClosable | null)?.close;
    if (typeof closer === "function") {
      await closer.call(conn);
    }
  }

  /** Should an error seen while the connection was borrowed retire it outright? Default: no. */
  isFatalError(_error: unknown): boolean {
    return false;
  }
}

export interface AsyncCallableFactoryOptions<T> {
  close?: (conn: T) => unknown;
  validate?: (conn: T) => unknown;
  reset?: (conn: T) => unknown;
  isFatalError?: (error: unknown) => boolean;
}

/** Build a factory from plain callables (sync or async both work). */
export class AsyncCallableFactory<T> extends AsyncConnectionFactory<T> {
  private readonly _create: () => T | Promise<T>;
  private readonly _close: ((conn: T) => unknown) | undefined;
  private readonly _validate: ((conn: T) => unknown) | undefined;
  private readonly _reset: ((conn: T) => unknown) | undefined;
  private readonly _isFatal: ((error: unknown) => boolean) | undefined;

  constructor(create: () => T | Promise<T>, options: AsyncCallableFactoryOptions<T> = {}) {
    super();
    this._create = create;
    this._close = options.close;
    this._validate = options.validate;
    this._reset = options.reset;
    this._isFatal = options.isFatalError;
  }

  create(): T | Promise<T> {
    return this._create();
  }

  override async validate(conn: T): Promise<boolean> {
    return this._validate === undefined ? true : Boolean(await this._validate(conn));
  }

  override async reset(conn: T): Promise<void> {
    if (this._reset) {
      await this._reset(conn);
    }
  }

  override async close(conn: T): Promise<void> {
    if (this._close) {
      await this._close(conn);
    } else {
      await super.close(conn);
    }
  }

  override isFatalError(error: unknown): boolean {
    return this._isFatal ? this._isFatal(error) : false;
  }
}
