package com.devexed.dbsource;


/**
 * Represents a database transaction. Extends {@link TransactionDatabase}
 * to support nesting of transactions.
 */
public interface Transaction extends TransactionDatabase {
	
	/** Commits the transaction. */
	void commit();
	
	/**
	 * Closes the transaction without committing it, rolling back any executed
	 * queries.
	 */
	@Override
	void close();
	
}
