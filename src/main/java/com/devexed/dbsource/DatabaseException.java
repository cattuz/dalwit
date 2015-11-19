package com.devexed.dbsource;

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
