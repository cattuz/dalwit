package com.devexed.dalwit;

/**
 * A closable interface which doesn't throw any checked exceptions.
 */
public interface Closeable extends java.io.Closeable {

    @Override
    void close();

}
