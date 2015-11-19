package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.Cursor;
import com.devexed.dbsource.ReadonlyDatabase;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * Can be used as a key accessor when the JDBC driver fully supports generated keys.
 */
public final class DefaultJdbcGeneratedKeysSelector implements JdbcGeneratedKeysSelector {

    @Override
    public PreparedStatement prepareInsertStatement(ReadonlyDatabase database, Connection connection, String sql,
                                                    Map<String, Class<?>> keyTypes) throws SQLException {
        return connection.prepareStatement(sql, keyTypes.keySet().toArray(new String[keyTypes.size()]));
    }

    @Override
    public Cursor selectGeneratedKeys(ReadonlyDatabase database, PreparedStatement statement,
                                      final JdbcAccessorFactory accessorFactory, final Map<String, Class<?>> keyTypes)
            throws SQLException {
        return new ResultSetCursor(new ResultSetCursor.TypeFunction() {

            @Override
            public Class<?> typeOf(String column) {
                return keyTypes.get(column);
            }

        }, accessorFactory, statement.getGeneratedKeys());
    }

}
