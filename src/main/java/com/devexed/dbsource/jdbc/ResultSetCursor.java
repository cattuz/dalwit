package com.devexed.dbsource.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.Function;

import com.devexed.dbsource.*;

final class ResultSetCursor extends AbstractCloseable implements Cursor {
	
	private static int columnIndexOf(int index) {
		return index + 1;
	}

    private final Function<String, JdbcAccessor> typeOfFunction;
	private final ResultSet cursor;
	
	ResultSetCursor(Function<String, JdbcAccessor> typeOfFunction, ResultSet cursor) {
		this.typeOfFunction = typeOfFunction;
		this.cursor = cursor;
	}
	
	@Override
	public boolean next() {
		checkNotClosed();
		
		try {
			return cursor.next();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

	@Override
    @SuppressWarnings("unchecked")
	public <T> T get(String column) {
		checkNotClosed();
		
		try {
            int index = cursor.findColumn(column);

            try {
                JdbcAccessor accessor = typeOfFunction.apply(column);

                if (accessor == null) throw new DatabaseException("No accessor is defined for column " + column);

				return (T) accessor.get(cursor, index);
			} catch (ClassCastException e) {
				throw new DatabaseException("Unsupported column type " +
                        cursor.getMetaData().getColumnTypeName(index), e);
			}
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    @Override
    public void close() {
        super.close();

        try {
            cursor.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }
}
