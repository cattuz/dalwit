package com.devexed.dalwit;

/**
 * A statement that modifies the database, tables or rows, in any way.
 */
public interface ExecutionStatement extends Statement {

    /**
     * Execute the statement on the database.
     * @param transaction The transaction on which to execute the statement.
     * @throws DatabaseException If the statement failed to execute.
     */
    void execute(Transaction transaction);

}
