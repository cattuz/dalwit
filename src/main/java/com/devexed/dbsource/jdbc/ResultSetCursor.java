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
	public interface AccessorFunction {

		JdbcAccessor accessorOf(String column);

	}

    private final AccessorFunction typeOfFunction;
	private final ResultSet resultSet;
	
	ResultSetCursor(AccessorFunction typeOfFunction, ResultSet resultSet) {
		this.typeOfFunction = typeOfFunction;
		this.resultSet = resultSet;
	}

	@Override
	public boolean seek(int rows) {
		checkNotClosed();

        try {
            if (resultSet.relative(rows)) {
                close();
                return true;
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        return false;
	}

    @Override
    public boolean previous() {
        checkNotClosed();

        try {
            if (resultSet.previous()) {
                close();
                return true;
            }
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

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
            // Should we cache the indexes or rely on the JDBC implementation to be swift enough?
            int index = resultSet.findColumn(column);
            JdbcAccessor accessor = typeOfFunction.accessorOf(column);

            if (accessor == null) throw new DatabaseException("No accessor is defined for column " + column);

            return (T) accessor.get(resultSet, index);
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

	@Override
	protected boolean isClosed() {
        // Override to report correct status if this result set was closed by, for example, its parent JDBC statement.
		try {
			return resultSet.isClosed() || super.isClosed();
		} catch (SQLException e) {
			throw new DatabaseException(e);
		}
	}

}
