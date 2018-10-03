package com.devexed.dalwit;

/**
 * Database supporting transactions (modifying the database).
 */
public interface Database extends ReadonlyDatabase {

    /**
     * Prepares a query into a statement that reads from or writes to the database.
     *
     * @param query The SQL statement.
     * @return The prepared query statement.
     * @throws DatabaseException If the statement could not be prepared.
     */
    @Override
    Statement prepare(Query query);

    /**
     * Start a transaction to update the database.
     *
     * @return The transaction which when committed will update the database.
     * @throws DatabaseException If the transaction could not be started.
     */
    Transaction transact();

}
