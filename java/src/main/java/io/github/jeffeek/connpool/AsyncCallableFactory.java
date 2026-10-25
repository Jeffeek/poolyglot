package io.github.jeffeek.connpool;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Build an {@link AsyncConnectionFactory} from plain lambdas. */
public final class AsyncCallableFactory<T> extends AsyncConnectionFactory<T> {
    private final Function<CancelToken, CompletableFuture<T>> create;
    private final BiFunction<T, CancelToken, CompletableFuture<Void>> close;
    private final BiFunction<T, CancelToken, CompletableFuture<Boolean>> validate;
    private final BiFunction<T, CancelToken, CompletableFuture<Void>> reset;
    private final Predicate<Throwable> isFatal;

    private AsyncCallableFactory(Builder<T> b) {
        this.create = b.create;
        this.close = b.close;
        this.validate = b.validate;
        this.reset = b.reset;
        this.isFatal = b.isFatal;
    }

    public static <T> Builder<T> builder(Function<CancelToken, CompletableFuture<T>> create) {
        return new Builder<>(create);
    }

    /** Convenience for a synchronous creator. */
    public static <T> Builder<T> fromSync(Supplier<T> create) {
        return new Builder<>(token -> CompletableFuture.completedFuture(create.get()));
    }

    @Override
    public CompletableFuture<T> createAsync(CancelToken token) {
        return create.apply(token);
    }

    @Override
    public CompletableFuture<Boolean> validateAsync(T conn, CancelToken token) {
        return validate == null ? CompletableFuture.completedFuture(true) : validate.apply(conn, token);
    }

    @Override
    public CompletableFuture<Void> resetAsync(T conn, CancelToken token) {
        return reset == null ? CompletableFuture.completedFuture(null) : reset.apply(conn, token);
    }

    @Override
    public CompletableFuture<Void> closeAsync(T conn, CancelToken token) {
        return close == null ? super.closeAsync(conn, token) : close.apply(conn, token);
    }

    @Override
    public boolean isFatalError(Throwable exc) {
        return isFatal != null && isFatal.test(exc);
    }

    public static final class Builder<T> {
        private final Function<CancelToken, CompletableFuture<T>> create;
        private BiFunction<T, CancelToken, CompletableFuture<Void>> close;
        private BiFunction<T, CancelToken, CompletableFuture<Boolean>> validate;
        private BiFunction<T, CancelToken, CompletableFuture<Void>> reset;
        private Predicate<Throwable> isFatal;

        private Builder(Function<CancelToken, CompletableFuture<T>> create) {
            this.create = create;
        }

        public Builder<T> close(BiFunction<T, CancelToken, CompletableFuture<Void>> value) {
            this.close = value;
            return this;
        }

        public Builder<T> closeSync(Consumer<T> value) {
            this.close = (conn, token) -> {
                value.accept(conn);
                return CompletableFuture.completedFuture(null);
            };
            return this;
        }

        public Builder<T> validate(BiFunction<T, CancelToken, CompletableFuture<Boolean>> value) {
            this.validate = value;
            return this;
        }

        public Builder<T> validateSync(Predicate<T> value) {
            this.validate = (conn, token) -> CompletableFuture.completedFuture(value.test(conn));
            return this;
        }

        public Builder<T> reset(BiFunction<T, CancelToken, CompletableFuture<Void>> value) {
            this.reset = value;
            return this;
        }

        public Builder<T> resetSync(Consumer<T> value) {
            this.reset = (conn, token) -> {
                value.accept(conn);
                return CompletableFuture.completedFuture(null);
            };
            return this;
        }

        public Builder<T> isFatalError(Predicate<Throwable> value) {
            this.isFatal = value;
            return this;
        }

        public AsyncCallableFactory<T> build() {
            return new AsyncCallableFactory<>(this);
        }
    }
}
