package com.devexed.dalwit;

/**
 * An exception which occurs when database access fails for any reason. Wraps underlying exception types thrown by the
 * database implementation.
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
