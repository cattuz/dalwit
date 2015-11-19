package com.devexed.dbsource;

/**
 * A statement that updates rows in a database. In SQL databases this typically includes insert, update, and delete
 * queries.
 */
public interface UpdateStatement extends Statement {

    long update(Transaction transaction);

}
