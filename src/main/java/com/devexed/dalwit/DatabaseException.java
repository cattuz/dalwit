package com.devexed.dalwit;

/**
 * An exception which occurs when the database access fails.
 */
public final class DatabaseException extends RuntimeException {

    public DatabaseException(String s) {
        super(s);
    }

    public DatabaseException(Throwable e) {
        super(e);
    }

    public DatabaseException(String s, Throwable e) {
        super(s, e);
    }

}
