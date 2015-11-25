package com.devexed.dalwit;

/**
 * An interface for objects which can close some specific type of resource.
 *
 * @param <E> The type of resource this closer can close.
 */
public interface Closer<E> {

    void close(E closeable);

}
