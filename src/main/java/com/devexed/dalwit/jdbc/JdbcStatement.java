package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Accessor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.Statement;
import com.devexed.dalwit.util.AbstractCloseable;

import java.sql.PreparedStatement;
import java.sql.SQLException;

abstract class JdbcStatement extends AbstractCloseable implements Statement {

    final Query query;
    final JdbcAbstractDatabase database;
    PreparedStatement statement;

    JdbcStatement(JdbcAbstractDatabase database, Query query) {
        this.database = database;
        this.query = query;
    }

    final void assignStatement(PreparedStatement statement) {
        this.statement = statement;
    }

    @Override
    public <T> Binder<T> binder(String parameter) {
        checkNotClosed();
        Class<?> parameterType = query.typeOf(parameter);
        int[] parameterIndices = query.indicesOf(parameter);

        return new JdbcBinder<>(statement, database.accessorFactory.create(parameterType), parameterIndices);
    }

    @Override
    public <T> void bind(String parameter, T value) {
        this.<T>binder(parameter).bind(value);
    }

    @Override
    protected final boolean isClosed() {
        try {
            return super.isClosed() || statement.isClosed();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public final void close() {
        try {
            statement.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }

        super.close();
    }

    private final class JdbcBinder<T> implements Binder<T> {

        private final PreparedStatement statement;
        private final Accessor<PreparedStatement, ?, SQLException> accessor;
        private final int[] indices;

        private JdbcBinder(PreparedStatement statement, Accessor<PreparedStatement, ?, SQLException> accessor, int[] indices) {
            this.statement = statement;
            this.accessor = accessor;
            this.indices = indices;
        }

        @Override
        public void bind(T value) {
            try {
                for (int index : indices) accessor.set(statement, index, value);
            } catch (SQLException e) {
                throw new DatabaseException(e);
            }
        }

    }

}
