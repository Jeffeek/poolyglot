package io.github.jeffeek.connpool;

import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Build a {@link ConnectionFactory} from plain lambdas. */
public final class CallableFactory<T> extends ConnectionFactory<T> {
    private final Supplier<T> create;
    private final Consumer<T> close;
    private final Predicate<T> validate;
    private final Consumer<T> reset;
    private final Predicate<Throwable> isFatal;

    private CallableFactory(Builder<T> b) {
        this.create = b.create;
        this.close = b.close;
        this.validate = b.validate;
        this.reset = b.reset;
        this.isFatal = b.isFatal;
    }

    public static <T> Builder<T> builder(Supplier<T> create) {
        return new Builder<>(create);
    }

    @Override
    public T create() {
        return create.get();
    }

    @Override
    public boolean validate(T conn) {
        return validate == null || validate.test(conn);
    }

    @Override
    public void reset(T conn) {
        if (reset != null) {
            reset.accept(conn);
        }
    }

    @Override
    public void close(T conn) {
        if (close != null) {
            close.accept(conn);
        } else {
            super.close(conn);
        }
    }

    @Override
    public boolean isFatalError(Throwable exc) {
        return isFatal != null && isFatal.test(exc);
    }

    public static final class Builder<T> {
        private final Supplier<T> create;
        private Consumer<T> close;
        private Predicate<T> validate;
        private Consumer<T> reset;
        private Predicate<Throwable> isFatal;

        private Builder(Supplier<T> create) {
            this.create = create;
        }

        public Builder<T> close(Consumer<T> value) {
            this.close = value;
            return this;
        }

        public Builder<T> validate(Predicate<T> value) {
            this.validate = value;
            return this;
        }

        public Builder<T> reset(Consumer<T> value) {
            this.reset = value;
            return this;
        }

        public Builder<T> isFatalError(Predicate<Throwable> value) {
            this.isFatal = value;
            return this;
        }

        public CallableFactory<T> build() {
            return new CallableFactory<>(this);
        }
    }
}
