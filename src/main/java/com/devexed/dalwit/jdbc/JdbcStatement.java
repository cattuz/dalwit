package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Accessor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.Statement;
import com.devexed.dalwit.util.AbstractCloseable;

import java.lang.reflect.Array;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

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
        Integer listSize = query.listSizeOf(parameter);

        if (listSize == null) {
            // Scalar parameter
            Class<?> parameterType = query.typeOf(parameter);
            int[] parameterIndices = query.indicesOf(parameter);
            Accessor<PreparedStatement, ResultSet, SQLException> accessor = database.accessorFactory.create(parameterType);

            if (accessor == null) {
                throw new DatabaseException("No accessor is defined for type " + parameterType + " (parameter " + parameter + ")");
            }

            return new JdbcBinder<>(statement, accessor, parameterIndices);
        } else {
            // List parameter
            ArrayList<Binder<Object>> binders = new ArrayList<>(listSize);

            for (int i = 0; i < listSize; i++) {
                binders.add(binder(Query.parameterListIndexer(parameter, i)));
            }

            return new JdbcListBinder<>(binders);
        }
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

    private static final class JdbcBinder<T> implements Binder<T> {

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

    private static final class JdbcListBinder<T> implements Binder<T> {

        private static final String countMismatchError = "List value must have exactly %d elements to match the declared parameter type";

        private final ArrayList<Binder<Object>> binders;

        private JdbcListBinder(ArrayList<Binder<Object>> binders) {
            this.binders = binders;
        }

        @Override
        public void bind(T value) {
            if (value == null) return;

            Class<?> valueType = value.getClass();

            if (Collection.class.isAssignableFrom(valueType)) {
                Collection values = (Collection) value;

                if (values.size() != binders.size()) {
                    throw new DatabaseException(String.format(countMismatchError, values.size()));
                }

                int i = 0;

                for (Object object : values) {
                    binders.get(i).bind(object);
                    i++;
                }
            } else if (valueType.isArray()) {
                int length = Array.getLength(value);

                if (length != binders.size()) {
                    throw new DatabaseException(String.format(countMismatchError, length));
                }

                for (int i = 0; i < length; i++) {
                    binders.get(i).bind(Array.get(value, i));
                }
            } else {
                throw new DatabaseException("Expected iterable type, was " + valueType);
            }
        }

    }

}
