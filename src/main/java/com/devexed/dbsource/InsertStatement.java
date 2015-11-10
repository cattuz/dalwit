package com.devexed.dbsource;

import java.util.List;

/**
 * A statement that inserts rows in a database and returns the keys it generates, if any.
 */
public interface InsertStatement extends Statement {

	Cursor insert(Transaction transaction);

}
