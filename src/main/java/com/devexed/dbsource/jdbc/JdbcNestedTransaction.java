package com.devexed.dbsource.jdbc;

import java.sql.SQLException;
import java.sql.Savepoint;

final class JdbcNestedTransaction extends JdbcTransaction {

    private final JdbcTransaction parent;
	private final Savepoint savepoint;

	/**
	 * Create a root level transaction. Committing this transaction will only
	 * update the database if the parent chain of transactions are committed.
	 */
	JdbcNestedTransaction(JdbcTransaction parent, Savepoint savepoint) {
		super(parent);
        this.parent = parent;
		this.savepoint = savepoint;
	}

	@Override
	void jdbcCommit() throws SQLException {
		connection.releaseSavepoint(savepoint);
	}

	@Override
	void jdbcRollback() throws SQLException {
		connection.rollback(savepoint);
	}

    @Override
    void jdbcClose() throws SQLException {
        parent.closeActiveTransaction();
    }

}
