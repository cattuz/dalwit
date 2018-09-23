package com.devexed.dalwit;

/**
 * A statement to query or modify the database.
 */
public interface Statement extends Closeable {

    <T> Binder<T> binder(String parameter);

    default <T> void bind(String parameter, T value) {
        this.<T>binder(parameter).bind(value);
    }

    interface Binder<T> {

        void bind(T value);

    }

}
