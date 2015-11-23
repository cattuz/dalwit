package com.devexed.dalwit;

/**
 * A statement that updates rows in a database. In SQL databases this typically includes INSERT, UPDATE, and DELETE
 * statements.
 */
public interface UpdateStatement extends Statement {

    /**
     * Execute the statement on the database, returning the number of rows affected.
     * @param transaction The transaction on which to execute the statement.
     * @return The number of rows affected.
     * @throws DatabaseException If the statement failed to execute.
     */
    long update(Transaction transaction);

}
