package com.devexed.dbsource;

public interface QueryStatement extends Statement {

    Cursor query(ReadonlyDatabase database);

    /**
     * Close a cursor opened by this statement.
     *
     * @param cursor The cursor to close.
     */
    void close(Cursor cursor);

}
