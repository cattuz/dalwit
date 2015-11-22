package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;
import com.devexed.dbsource.util.CloseableManager;

import java.sql.SQLException;

final class JdbcQueryStatement extends JdbcStatement implements QueryStatement {

    private final CloseableManager<ResultSetCursor> cursorManager =
            new CloseableManager<ResultSetCursor>(QueryStatement.class, Cursor.class);

    public JdbcQueryStatement(JdbcAbstractDatabase database, Query query) {
        super(database, query);

        try {
            setStatement(database.connection.prepareStatement(query.create(database, parameterIndexes)));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Cursor query(ReadonlyDatabase database) {
        checkNotClosed();
        checkActiveDatabase(database);

        try {
            ResultSetCursor cursor = new ResultSetCursor(statement.executeQuery(), new ResultSetCursor.TypeFunction() {

                @Override
                public Class<?> typeOf(String column) {
                    return query.typeOf(column);
                }

            }, this.database.accessorFactory);
            cursorManager.open(cursor);
            return cursor;
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void close(Cursor cursor) {
        cursorManager.close(cursor);
    }

    @Override
    public void close() {
        cursorManager.close();
        super.close();
    }

}
