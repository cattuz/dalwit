package com.devexed.dbsource;

import java.util.Map;

/** Database supporting transactions (modifying the database). */
public interface TransactionDatabase extends Database {

	/**
	 * Prepares an SQL statement which updates the database and returns the update count.
	 *
	 * @param query The SQL statement. E.g. insert, update, delete.
	 * @return The prepared statement.
	 */
	UpdateStatement createUpdate(Query query);

	/**
	 * Prepares an SQL statement which executes a statement on
	 * the database.
	 *
	 * @param query The SQL statement.
	 * @return The prepared insert statement.
	 */
	ExecutionStatement createExecution(Query query);

	/**
	 * Prepares an SQL statement which inserts rows into a table,
	 * returning a cursor to the generated keys when executed.
	 *
	 * @param query The SQL insert statement.
	 * @param keys The key columns to return in the cursor of generated keys.
	 * @return The prepared insert statement.
	 */
	InsertStatement createInsert(Query query, Map<String, Class<?>> keys);

	/**
	 * Start a transaction to update the database.
	 *
	 * @return The transaction which when committed will update the database.
	 */
	Transaction transact();
	
}
