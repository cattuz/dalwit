package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Transaction;

import java.sql.SQLException;

final class JdbcRootTransaction extends JdbcTransaction {

	/**
	 * Create a root level transaction. Committing this transaction will
	 * update the database.
	 */
	JdbcRootTransaction(JdbcAbstractDatabase parent) {
		super(parent);
	}

    @Override
    void jdbcCommit() throws SQLException {
        connection.commit();
    }

    @Override
    void jdbcRollback() throws SQLException {
        connection.rollback();
    }

    @Override
    void jdbcClose() throws SQLException {}

}
