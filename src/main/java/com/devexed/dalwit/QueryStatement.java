package com.devexed.dalwit;

public interface QueryStatement extends Statement {

    /**
     * Query the database using this statement, returning a cursor over the rows returned.
     * @param database The database which to query.
     * @return The cursor over the rows returned.
     * @throws DatabaseException If the statement failed to execute.
     */
    Cursor query(ReadonlyDatabase database);

    /**
     * Close a cursor opened by this statement. Also, closes all the cursor's resources. If <code>cursor</code> is null
     * this is a no-op.
     *
     * @param cursor The cursor to close.
     */
    void close(Cursor cursor);

}
