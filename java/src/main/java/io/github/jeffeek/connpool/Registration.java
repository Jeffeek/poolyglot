package io.github.jeffeek.connpool;

/** Handle returned by {@link CancelToken#register}; close it to detach the callback. */
public interface Registration extends AutoCloseable {
    Registration NOOP = () -> { };

    @Override
    void close();
}
