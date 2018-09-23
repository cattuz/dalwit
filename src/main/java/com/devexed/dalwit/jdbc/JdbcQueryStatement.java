package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.QueryStatement;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

final class JdbcQueryStatement extends JdbcStatement implements QueryStatement {

    JdbcQueryStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            assignStatement(database.connection.prepareStatement(query.createSql()));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Cursor query() {
        checkNotClosed();
        database.checkActive();

        try {
            Map<String, Cursor.Getter<?>> columns = new HashMap<>();
            ResultSet resultSet = statement.executeQuery();
            ResultSetMetaData resultSetMetaData = resultSet.getMetaData();

            for (int i = 0, l = resultSetMetaData.getColumnCount(); i < l; i++) {
                String column = resultSetMetaData.getColumnName(i + 1);
                Class<?> columnType = query.typeOf(column);
                columns.put(column.toLowerCase(), new ResultSetCursor.ResultSetGetter(database.accessorFactory.create(columnType), resultSet, i));
            }

            return new ResultSetCursor(resultSet, columns);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
