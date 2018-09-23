package com.devexed.dalwit;

/**
 * A statement to query or modify the database.
 */
public interface Statement extends Closeable {

    <T> Binder<T> binder(String parameter);

    <T> void bind(String parameter, T value);

    interface Binder<T> {

        void bind(T value);

    }

}
