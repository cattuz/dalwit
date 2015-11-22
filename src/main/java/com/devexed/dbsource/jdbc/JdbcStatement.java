package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;
import com.devexed.dbsource.util.AbstractCloseable;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

abstract class JdbcStatement extends AbstractCloseable implements Statement {

    final JdbcAbstractDatabase database;
    final Query query;
    final HashMap<String, ArrayList<Integer>> parameterIndexes;
    PreparedStatement statement;

    JdbcStatement(JdbcAbstractDatabase database, Query query) {
        this.database = database;
        this.query = query;
        this.parameterIndexes = new HashMap<String, ArrayList<Integer>>();
    }

    final void setStatement(PreparedStatement statement) {
        this.statement = statement;
    }

    void checkActiveDatabase(ReadonlyDatabase database) {
        if (!(database instanceof JdbcAbstractDatabase))
            throw new DatabaseException("Expecting JDBC database");

        ((JdbcAbstractDatabase) database).checkActive();
    }

    void checkActiveTransaction(Transaction transaction) {
        if (!(transaction instanceof JdbcTransaction))
            throw new DatabaseException("Expecting JDBC transaction");

        ((JdbcTransaction) transaction).checkActive();
    }

    @Override
    public void clear() {
        try {
            statement.clearParameters();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public <T> void bind(String parameter, T value) {
        checkNotClosed();

        try {
            Class<?> type = query.typeOf(parameter);
            if (type == null) throw new DatabaseException("No such parameter " + parameter);

            ArrayList<Integer> indexes = parameterIndexes.get(parameter);
            if (indexes == null) throw new DatabaseException("No mapping for parameter " + parameter);

            Accessor<PreparedStatement, ResultSet, SQLException> accessor = database.accessorFactory.create(type);
            for (int index : indexes) accessor.set(statement, index, value);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close() {
        super.close();

        try {
            statement.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
