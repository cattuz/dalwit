package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.AccessorFactory;
import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Can be used as a key accessor when the JDBC driver fully supports generated keys.
 */
public final class DefaultJdbcGeneratedKeysSelector implements JdbcGeneratedKeysSelector {

    @Override
    public PreparedStatement prepareInsertStatement(Connection connection, String sql,
                                                    Map<String, Class<?>> keyTypes) throws SQLException {
        return connection.prepareStatement(sql, keyTypes.keySet().toArray(new String[0]));
    }

    @Override
    public Cursor selectGeneratedKeys(Connection connection, PreparedStatement statement,
                                      AccessorFactory<PreparedStatement, ResultSet, SQLException> accessorFactory,
                                      Map<String, Class<?>> keyTypes) throws SQLException {
        Map<String, Cursor.Getter<?>> columns = new HashMap<>();
        ResultSet resultSet = statement.getGeneratedKeys();
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

        for (int i = 0, l = resultSetMetaData.getColumnCount(); i < l; i++) {
            String column = resultSetMetaData.getColumnName(i + 1);
            Class<?> keyType = keyTypes.get(column);

            if (keyType == null) throw new DatabaseException("Missing type for generated key column " + column);

            columns.put(column.toLowerCase(), new ResultSetCursor.ResultSetGetter(accessorFactory.create(keyType), resultSet, i));
        }

        return new ResultSetCursor(resultSet, columns);
    }

}
