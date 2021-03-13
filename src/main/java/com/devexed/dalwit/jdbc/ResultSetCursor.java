package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Accessor;
import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.util.AbstractCloseable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * A cursor over a JDBC result set.
 */
public final class ResultSetCursor extends AbstractCloseable implements Cursor {

    private final ResultSet resultSet;
    public final Map<String, Getter<?>> columns;

    ResultSetCursor(ResultSet resultSet, Map<String, Getter<?>> columns) {
        this.resultSet = resultSet;
        this.columns = columns;
    }

    @Override
    public boolean seek(int rows) {
        checkNotClosed();

        try {
            return resultSet.relative(rows);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean previous() {
        checkNotClosed();

        try {
            return resultSet.previous();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public boolean next() {
        checkNotClosed();

        try {
            return resultSet.next();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Getter<T> getter(String column) {
        return (Getter<T>) columns.get(column.toLowerCase());
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

    static final class ResultSetGetter implements Cursor.Getter<Object> {

        private final Accessor<PreparedStatement, ResultSet, SQLException> accessor;
        private final ResultSet resultSet;
        private final int index;

        ResultSetGetter(Accessor<PreparedStatement, ResultSet, SQLException> accessor, ResultSet resultSet, int index) {
            this.accessor = accessor;
            this.resultSet = resultSet;
            this.index = index;
        }

        @Override
        public Object get() {
            try {
                return accessor.get(resultSet, index);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

    }
}
