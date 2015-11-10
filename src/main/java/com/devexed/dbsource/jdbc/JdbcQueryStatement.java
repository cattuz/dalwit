package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;

import java.sql.SQLException;
import java.util.function.Function;

final class JdbcQueryStatement extends JdbcStatement implements QueryStatement {

	public JdbcQueryStatement(JdbcAbstractDatabase database, Query query) {
		super(database, query);
	}

	@Override
	public Cursor query() {
		checkNotClosed();

		try {
			return new ResultSetCursor(new Function<String, JdbcAccessor>() {

				@Override
				public JdbcAccessor apply(String column) {
					return database.accessors.get(query.typeOf(column));
				}

			}, statement.executeQuery());
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
