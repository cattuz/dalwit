package com.devexed.dbsource;

/**
 * A statement that inserts rows in a database and returns the keys it generates, if any.
 */
public interface InsertStatement extends Statement {

    Cursor insert(Transaction transaction);

    /**
     * Close a cursor opened by this statement.
     *
     * @param cursor The cursor to close.
     */
    void close(Cursor cursor);

}
