package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

abstract class JdbcStatement extends AbstractCloseable implements Statement {

    final JdbcAbstractDatabase database;
    final Query query;
	final PreparedStatement statement;
    final LinkedHashMap<String, Class<?>> keys;

    private final HashMap<String, int[]> parameterIndexes;

    JdbcStatement(JdbcAbstractDatabase database, Query query, Map<String, Class<?>> keys) {
        this.database = database;
        this.query = query;

        try {
            parameterIndexes = new HashMap<String, int[]>();
            this.keys = new LinkedHashMap<String, Class<?>>();

            if (keys != null) this.keys.putAll(keys);

            statement = (keys != null)
                    ? database.generatedKeysSelector.prepareInsertStatement(database, database.connection,
                            query.create(database, parameterIndexes), keys)
                    : database.connection.prepareStatement(query.create(database, parameterIndexes));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    JdbcStatement(JdbcAbstractDatabase database, Query query) {
        this(database, query, null);
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
            JdbcAccessor accessor = database.accessors.get(type);

            if (accessor == null) throw new DatabaseException("No accessor is defined for type " + type);

            int[] indexes = parameterIndexes.get(parameter);

            if (indexes == null) throw new DatabaseException("No mapping for parameter " + parameter);

			for (int index: indexes) accessor.set(statement, index + 1, value);
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
