package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.Database;
import com.devexed.dbsource.DatabaseException;

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

    private void assertSingletonKeyMap(Map<String, Class<?>> keys) {
        if (keys.size() > 1)
            throw new DatabaseException("Only a single generated key column is supported.");
    }

    @Override
    public PreparedStatement prepareInsertStatement(Database database, Connection connection, String sql,
                                                    Map<String, Class<?>> keys) throws SQLException {
        // No keys need to be returned as the selection of generated keys is manual.
        assertSingletonKeyMap(keys);
        return connection.prepareStatement(sql);
    }

    @Override
    public Cursor selectGeneratedKeys(Database database, PreparedStatement statement,
                                      final Map<Class<?>, JdbcAccessor> accessors,
                                      final Map<String, Class<?>> keys) throws SQLException {
        assertSingletonKeyMap(keys);
        String key = keys.keySet().iterator().next();
        Class<?> type = keys.get(key);

        if (type != Long.TYPE) throw new DatabaseException("Generated key column must be of Long.TYPE");

        // Select last inserted id as key.
        ResultSet results = statement.getConnection().createStatement().executeQuery(
                "SELECT " + lastGeneratedIdFunction + " AS " +
                "\"" + key.replace("\"", "\"\"") + "\"");

        return new ResultSetCursor(new ResultSetCursor.AccessorFunction() {

            @Override
            public JdbcAccessor accessorOf(String name) {
                return accessors.get(keys.get(name));
            }

        }, results);
    }

}
