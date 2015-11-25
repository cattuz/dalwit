package com.devexed.dalwit;

public interface QueryStatement extends Statement {

    /**
     * Query the database using this statement, returning a cursor over the rows returned.
     * @return The cursor over the rows returned.
     * @throws DatabaseException If the statement failed to execute.
     */
    Cursor query();

}
