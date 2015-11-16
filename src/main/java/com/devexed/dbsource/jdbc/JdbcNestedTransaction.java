package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;

import java.sql.SQLException;
import java.sql.Savepoint;

/**
 * A JDBC transaction within a parent transaction. Implements transaction nesting using JDBC savepoints.
 */
final class JdbcNestedTransaction extends JdbcTransaction {

    private final JdbcTransaction parent;
	private final Savepoint savepoint;

	/**
	 * Create a root level transaction. Committing this transaction will only
	 * update the database if the parent chain of transactions are committed.
	 */
	JdbcNestedTransaction(JdbcTransaction parent) {
		super(parent);
        this.parent = parent;

		try {
			this.savepoint = parent.connection.setSavepoint();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

	@Override
	void commitTransaction() throws SQLException {
		connection.releaseSavepoint(savepoint);
	}

	@Override
	void rollbackTransaction() throws SQLException {
		connection.rollback(savepoint);
	}

	@Override
	public void close() {
        if (isClosed()) return;

		parent.closeActiveTransaction();
        super.close();
	}

}
