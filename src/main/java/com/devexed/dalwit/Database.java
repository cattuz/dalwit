package com.devexed.dalwit;

import java.util.Map;

/**
 * Database supporting transactions (modifying the database).
 */
public interface Database extends ReadonlyDatabase {

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
     * Start a transaction to update the database.
     *
     * @return The transaction which when committed will update the database.
     * @throws DatabaseException If the transaction could not be started.
     */
    Transaction transact();

    /**
     * Commit the transaction previously started by this database.
     *
     * @param transaction The transaction previously started by this database.
     * @throws DatabaseException If the transaction could not be committed.
     */
    void commit(Transaction transaction);

    /**
     * Roll back the transaction previously started by this database. If transaction is null this is a no-op.
     *
     * @param transaction The transaction previously started by this database.
     * @throws DatabaseException If the transaction could not be rolled back.
     */
    void rollback(Transaction transaction);

}
