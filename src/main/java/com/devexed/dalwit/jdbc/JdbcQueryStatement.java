package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.Query;
import com.devexed.dalwit.QueryStatement;

import java.sql.SQLException;

final class JdbcQueryStatement extends JdbcStatement implements QueryStatement {

    public JdbcQueryStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            setStatement(database.connection.prepareStatement(query.create(database, parameterIndexes, indexParameters)));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Cursor query() {
        checkNotClosed();
        database.checkActive();

        try {
            return new ResultSetCursor(statement.executeQuery(), new ResultSetCursor.TypeFunction() {

                @Override
                public Class<?> typeOf(String column) {
                    return query.typeOf(column);
                }

            }, this.database.accessorFactory);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
