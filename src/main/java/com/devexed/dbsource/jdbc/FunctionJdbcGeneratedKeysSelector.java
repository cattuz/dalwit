package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * <p>Incomplete implementation of generated key selection where a single function is used to get the identity value of
 * only the very last inserted id. Useful for databases whose JDBC implementation doesn't have getGeneratedKeys
 * support.</p>
 *
 * <p>For example, for SQLite one could use <code>new FunctionJdbcGeneratedKeysSelector("last_insert_rowid()")</code>.</p>
 */
public final class FunctionJdbcGeneratedKeysSelector implements GeneratedKeysSelector {

    private final String lastGeneratedIdFunction;
    private final Class<?> lastGeneratedIdType;

    public FunctionJdbcGeneratedKeysSelector(String lastGeneratedIdFunction, Class<?> lastGeneratedIdType) {
        this.lastGeneratedIdFunction = lastGeneratedIdFunction;
        this.lastGeneratedIdType = lastGeneratedIdType;
    }

    @Override
    public PreparedStatement prepareInsertStatement(Database database, Connection connection, String sql,
                                                    Map<String, Class<?>> keys) throws SQLException {
        if (keys.size() > 1) throw new DatabaseException("Only a single generated key column is supported.");

        String key = keys.keySet().iterator().next();
        Class<?> type = keys.get(key);

        if (!type.equals(lastGeneratedIdType))
            throw new DatabaseException("Generated key column must be a " + lastGeneratedIdType);

        return connection.prepareStatement(sql);
    }

    @Override
    public Cursor selectGeneratedKeys(Database database, PreparedStatement statement,
                                      final JdbcAccessorFactory accessorFactory,
                                      final Map<String, Class<?>> keys) throws SQLException {
        // Select last inserted id as key.
        final String key = keys.keySet().iterator().next();
        final Object generatedKey;
        ResultSet results = null;

        try {
            results = statement.getConnection().createStatement().executeQuery("SELECT " + lastGeneratedIdFunction);
            if (!results.next()) return EmptyCursor.of();

            JdbcAccessor accessor = accessorFactory.create(lastGeneratedIdType);

            if (accessor == null) throw new DatabaseException("No accessor is defined for " + lastGeneratedIdType);

            generatedKey = accessor.get(results, 0);
        } finally {
            if (results != null) results.close();
        }

        return Cursors.singleton(new Cursors.ColumnFunction() {

            @Override
            @SuppressWarnings("unchecked")
            public <E> E get(String column) {
                return (E) generatedKey;
            }

        });
    }

}
