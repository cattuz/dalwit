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

        /*try {
            parameterIndexes = new HashMap<String, ArrayList<Integer>>();
            this.keys = new LinkedHashMap<String, Class<?>>();

            if (keys != null) this.keys.putAll(keys);

            statement = (keys != null)
                    ? database.generatedKeysSelector.prepareInsertStatement(database, database.connection,
                    query.create(database, parameterIndexes), keys)
                    : database.connection.prepareStatement(query.create(database, parameterIndexes));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }*/
    }

    /*JdbcStatement(JdbcAbstractDatabase database, Query query) {
        this(database, query, null);
    }*/

    final void setStatement(PreparedStatement statement) {
        this.statement = statement;
    }

    void checkActiveDatabase(ReadonlyDatabase database) {
        if (!(database instanceof JdbcAbstractDatabase))
            throw new DatabaseException("Expecting " + ReadonlyDatabase.class + " not " + database.getClass());

        ((JdbcAbstractDatabase) database).checkActive();
    }

    void checkActiveTransaction(Transaction transaction) {
        if (!(transaction instanceof JdbcTransaction))
            throw new DatabaseException("Expecting " + JdbcTransaction.class + " not " + transaction.getClass());

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
