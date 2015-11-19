package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.AbstractCloseable;
import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.DatabaseException;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A cursor over a JDBC result set.
 */
final class ResultSetCursor extends AbstractCloseable implements Cursor {

    /** Interface providing method of getting the accessor of a column with a specific name. */
	public interface TypeFunction {

		Class<?> typeOf(String column);

	}

    private final TypeFunction typeOfFunction;
    private final JdbcAccessorFactory accessorFactory;
	private final ResultSet resultSet;
	
	ResultSetCursor(TypeFunction typeOfFunction, JdbcAccessorFactory accessorFactory, ResultSet resultSet) {
		this.typeOfFunction = typeOfFunction;
        this.accessorFactory = accessorFactory;
        this.resultSet = resultSet;
	}

	@Override
	public boolean seek(int rows) {
		checkNotClosed();

        try {
            if (resultSet.relative(rows)) return true;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        close();

        return false;
	}

    @Override
    public boolean previous() {
        checkNotClosed();

        try {
            if (resultSet.previous()) return true;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        close();

        return false;
    }

    @Override
    public boolean next() {
        checkNotClosed();

        try {
            if (resultSet.next()) return true;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

		close();

        return false;
    }

	@Override
    @SuppressWarnings("unchecked")
	public <T> T get(String column) {
		checkNotClosed();
		
		try {
            Class<?> type = typeOfFunction.typeOf(column);
            if (type == null) throw new DatabaseException("No such column " + column);

            JdbcAccessor accessor = accessorFactory.create(type);
            int index = resultSet.findColumn(column) - 1;

            return (T) accessor.get(resultSet, index);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

    @Override
    public void close() {
        try {
            resultSet.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        super.close();
    }
}
