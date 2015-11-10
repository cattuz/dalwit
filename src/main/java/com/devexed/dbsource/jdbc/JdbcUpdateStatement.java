package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;

import java.sql.Connection;
import java.sql.SQLException;

final class JdbcUpdateStatement extends JdbcStatement implements UpdateStatement {

	public JdbcUpdateStatement(JdbcAbstractDatabase database, Query query) {
		super(database, query);
	}

	@Override
	public long update(Transaction transaction) {
		checkNotClosed();
		checkActiveTransaction(transaction);

		try {
			return statement.executeUpdate();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
