package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.DatabaseCursor;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.QueryStatement;

import java.sql.SQLException;

final class JdbcQueryStatement extends JdbcStatement implements QueryStatement {

	public JdbcQueryStatement(JdbcAbstractDatabase database, Query query) {
		super(database, query);
	}

	@Override
	public DatabaseCursor query() {
		checkNotClosed();

		try {
			return new ResultSetCursor(new ResultSetCursor.AccessorFunction() {

				@Override
				public JdbcAccessor accessorOf(String column) {
					return database.accessors.get(query.typeOf(column));
				}

			}, statement.executeQuery());
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
