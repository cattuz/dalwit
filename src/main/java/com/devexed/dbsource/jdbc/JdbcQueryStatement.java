package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.DatabaseException;
import com.devexed.dbsource.Query;
import com.devexed.dbsource.QueryStatement;

import java.sql.SQLException;

final class JdbcQueryStatement extends JdbcStatement implements QueryStatement {

	public JdbcQueryStatement(JdbcAbstractDatabase database, Query query) {
		super(database, query);
	}

	@Override
	public Cursor query() {
		checkNotClosed();

		try {
			return new ResultSetCursor(new ResultSetCursor.TypeFunction() {

				@Override
				public Class<?> typeOf(String column) {
					return query.typeOf(column);
				}

			}, database.accessorFactory, statement.executeQuery());
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
