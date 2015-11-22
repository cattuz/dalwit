package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.AccessorFactory;
import com.devexed.dbsource.util.CloseableCursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * Can be used as a key accessor when the JDBC driver fully supports generated keys.
 */
public final class DefaultJdbcGeneratedKeysSelector implements JdbcGeneratedKeysSelector {

    @Override
    public PreparedStatement prepareInsertStatement(Connection connection, String sql,
                                                    Map<String, Class<?>> keyTypes) throws SQLException {
        return connection.prepareStatement(sql, keyTypes.keySet().toArray(new String[keyTypes.size()]));
    }

    @Override
    public CloseableCursor selectGeneratedKeys(Connection connection, PreparedStatement statement,
                                               AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                                               Map<String, Class<?>> keyTypes) throws SQLException {
        return new ResultSetCursor(statement.getGeneratedKeys(), new KeyTypeFunction(keyTypes), accessorFactory);
    }

    private static class KeyTypeFunction implements ResultSetCursor.TypeFunction {

        private final Map<String, Class<?>> keyTypes;

        public KeyTypeFunction(Map<String, Class<?>> keyTypes) {
            this.keyTypes = keyTypes;
        }

        @Override
        public Class<?> typeOf(String column) {
            return keyTypes.get(column);
        }

    }

}
