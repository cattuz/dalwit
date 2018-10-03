package com.devexed.dalwit;

/**
 * A statement to query or modify the database.
 */
public interface ReadonlyStatement extends Closeable {

    /**
     * Query the database using this statement, returning a cursor over the rows returned.
     * @return The cursor over the rows returned.
     * @throws DatabaseException If the statement failed to execute.
     */
    Cursor query();

    <T> Binder<T> binder(String parameter);

    default <T> void bind(String parameter, T value) {
        this.<T>binder(parameter).bind(value);
    }

    interface Binder<T> {

        void bind(T value);

    }

}
