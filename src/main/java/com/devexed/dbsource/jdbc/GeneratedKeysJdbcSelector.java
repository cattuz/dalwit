package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.Database;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.function.Function;

/**
 * Can be used as a key accessor when the JDBC driver fully supports generated keys.
 */
public final class GeneratedKeysJdbcSelector implements GeneratedKeysSelector {

    @Override
    public PreparedStatement prepareInsertStatement(Database database, Connection connection, String sql,
                                                    Map<String, Class<?>> keys) throws SQLException {
        return connection.prepareStatement(sql, keys.keySet().toArray(new String[keys.size()]));
    }

    @Override
    public Cursor selectGeneratedKeys(Database database, PreparedStatement statement,
                                      final Map<Class<?>, JdbcAccessor> accessors, final Map<String, Class<?>> keys)
            throws SQLException {
        return new ResultSetCursor(new Function<String, JdbcAccessor>() {

            @Override
            public JdbcAccessor apply(String name) {
                return accessors.get(keys.get(name));
            }

        }, statement.getGeneratedKeys());
    }

}
