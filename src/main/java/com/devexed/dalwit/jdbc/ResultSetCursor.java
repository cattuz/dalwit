package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Accessor;
import com.devexed.dalwit.AccessorFactory;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.util.AbstractCloseable;
import com.devexed.dalwit.util.CloseableCursor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * A cursor over a JDBC result set.
 */
final class ResultSetCursor extends AbstractCloseable implements CloseableCursor {

    private final ResultSet resultSet;
    private final TypeFunction typeOfFunction;
    private final AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory;

    ResultSetCursor(ResultSet resultSet, TypeFunction typeOfFunction,
                    AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory) {
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

            Accessor<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessor = accessorFactory.create(type);
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

    /**
     * Interface providing method of getting the accessor of a column with a specific name.
     */
    public interface TypeFunction {

        Class<?> typeOf(String column);

    }
}
