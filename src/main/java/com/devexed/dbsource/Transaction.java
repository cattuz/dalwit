package com.devexed.dbsource;


/**
 * Represents a database transaction. Extends {@link Database}
 * to support nesting of transactions.
 */
public interface Transaction extends Database {

    /**
     * Commits the transaction.
     */
    void commit();

    /**
     * Closes the transaction without committing it, rolling back any executed
     * queries.
     */
    @Override
    void close();

}
