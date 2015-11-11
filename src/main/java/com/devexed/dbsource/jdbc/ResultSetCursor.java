package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.DatabaseCursor;
import com.devexed.dbsource.DatabaseException;

import java.sql.ResultSet;
import java.sql.SQLException;

final class ResultSetCursor extends AbstractCloseable implements DatabaseCursor {

	public interface AccessorFunction {

		JdbcAccessor accessorOf(String column);

	}
	
	private static int columnIndexOf(int index) {
		return index + 1;
	}

    private final AccessorFunction typeOfFunction;
	private final ResultSet cursor;
	
	ResultSetCursor(AccessorFunction typeOfFunction, ResultSet cursor) {
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
                JdbcAccessor accessor = typeOfFunction.accessorOf(column);

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
