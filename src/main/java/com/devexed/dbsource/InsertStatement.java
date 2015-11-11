package com.devexed.dbsource;

/**
 * A statement that inserts rows in a database and returns the keys it generates, if any.
 */
public interface InsertStatement extends Statement {

	DatabaseCursor insert(Transaction transaction);

}
