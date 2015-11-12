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
 * <p>For example, for SQLite one could create a instance using the <code>last_insert_rowid()</code> sql function to get
 * the last id inserted.</p>
 */
public final class GeneratedKeysFunctionSelector implements GeneratedKeysSelector {

    private final String lastGeneratedIdFunction;

    public GeneratedKeysFunctionSelector(String lastGeneratedIdFunction) {
        this.lastGeneratedIdFunction = lastGeneratedIdFunction;
    }

    @Override
    public PreparedStatement prepareInsertStatement(Database database, Connection connection, String sql,
                                                    Map<String, Class<?>> keys) throws SQLException {
        if (keys.size() > 1) throw new DatabaseException("Only a single generated key column is supported.");

        String key = keys.keySet().iterator().next();
        Class<?> type = keys.get(key);

        if (type != Long.TYPE) throw new DatabaseException("Generated key column must be of type Long.TYPE");

        return connection.prepareStatement(sql);
    }

    @Override
    public DatabaseCursor selectGeneratedKeys(Database database, PreparedStatement statement,
                                      final Map<Class<?>, JdbcAccessor> accessors,
                                      final Map<String, Class<?>> keys) throws SQLException {
        // Select last inserted id as key.
        final String key = keys.keySet().iterator().next();
        final long generatedKey;
        ResultSet results = null;

        try {
            results = statement.getConnection().createStatement().executeQuery("SELECT " + lastGeneratedIdFunction);
            if (!results.next()) return EmptyCursor.of();
            generatedKey = results.getLong(1);
        } finally {
            if (results != null) results.close();
        }

        return new MockCursor<Long>(new MockCursor.Getter() {

            @Override
            public boolean next(int index) {
                return index < 0;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <E> E get(int index, String column) {
                if (!key.equals(column))
                    throw new DatabaseException("Column must be key column " + key);

                return (E) (Long) generatedKey;
            }

        });
    }

}
