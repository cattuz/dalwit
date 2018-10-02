package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;

import java.sql.PreparedStatement;
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
                String column = database.columnNameMapper.apply(resultSetMetaData.getColumnName(i + 1));
                Class<?> columnType = query.typeOf(column);
                Accessor<PreparedStatement, ResultSet, SQLException> accessor = database.accessorFactory.create(columnType);

                if (accessor == null) {
                    throw new DatabaseException("No accessor is defined for type " + columnType + " (column " + column + ")");
                }

                columns.put(column.toLowerCase(), new ResultSetCursor.ResultSetGetter(accessor, resultSet, i));
            }

            return new ResultSetCursor(resultSet, columns);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
