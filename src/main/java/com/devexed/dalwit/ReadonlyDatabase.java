package com.devexed.dalwit;

public interface ReadonlyDatabase extends Closeable {

    /**
     * Prepares a query into a statement that reads from the database.
     *
     * @param query The SQL statement.
     * @return The prepared query statement.
     * @throws DatabaseException If the statement could not be prepared.
     */
    ReadonlyStatement prepare(Query query);

    /**
     * Start a transaction to read transactionally from the database.
     *
     * @return The transaction within which to read from the database.
     * @throws DatabaseException If the transaction could not be started.
     */
    ReadonlyTransaction transact();

}
