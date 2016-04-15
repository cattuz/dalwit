package com.devexed.dalwit;


import java.util.Map;

/**
 * Represents a database transaction. Updates to a {@link Database} can only occur within an open transaction.
 * Statements opened by a method on a transaction object are opened, closed and reusable in the database's "scope".
 */
public interface Transaction extends Database {

    /**
     * Prepares an SQL statement which executes a statement on the database.
     *
     * @param query The SQL statement.
     * @return The prepared insert statement.
     * @throws DatabaseException If the statement could not be prepared.
     */
    ExecutionStatement createExecution(Query query);

    /**
     * Prepares an SQL statement which updates the database and returns the update count.
     *
     * @param query The SQL statement. E.g. INSERT, UPDATE, DELETE.
     * @return The prepared statement.
     * @throws DatabaseException If the statement could not be prepared.
     */
    UpdateStatement createUpdate(Query query);

    /**
     * Prepares an SQL statement which inserts rows into a table and returns a cursor to the generated keys when
     * executed.
     *
     * @param query The SQL insert statement. Typically an INSERT statement.
     * @param keys  The key columns to return in the cursor of generated keys.
     * @return The prepared insert statement.
     * @throws DatabaseException If the statement could not be prepared.
     */
    InsertStatement createInsert(Query query, Map<String, Class<?>> keys);

    /**
     * Commit the transaction. If the transaction is not committed before it is closed is will be rolled back.
     *
     * @throws DatabaseException If the transaction could not be committed.
     */
    void commit();

}
