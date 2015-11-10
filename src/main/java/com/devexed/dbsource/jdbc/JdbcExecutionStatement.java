package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;

import java.sql.Connection;
import java.sql.SQLException;

final class JdbcExecutionStatement extends JdbcStatement implements ExecutionStatement {

	public JdbcExecutionStatement(JdbcAbstractDatabase database, Query query) {
		super(database, query);
	}

	@Override
	public void execute(Transaction transaction) {
		checkNotClosed();
		checkActiveTransaction(transaction);

		try {
			statement.execute();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
