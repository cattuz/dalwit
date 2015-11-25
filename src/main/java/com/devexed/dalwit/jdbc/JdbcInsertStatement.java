package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseableCloser;
import com.devexed.dalwit.util.CloseableCursor;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

final class JdbcInsertStatement extends JdbcStatement implements InsertStatement {

    private final LinkedHashMap<String, Class<?>> keys;
    private final AbstractCloseableCloser<Cursor, CloseableCursor> cursorManager =
            new AbstractCloseableCloser<Cursor, CloseableCursor>(InsertStatement.class, Cursor.class);

    public JdbcInsertStatement(JdbcAbstractDatabase database, Query query, Map<String, Class<?>> keys) {
        super(database, query);

        try {
            this.keys = new LinkedHashMap<String, Class<?>>(keys);
            setStatement(database.generatedKeysSelector.prepareInsertStatement(
                    database.connection, query.create(database, parameterIndexes, indexParameters), keys));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Cursor insert(Transaction transaction) {
        checkNotClosed();
        checkActiveTransaction(transaction);

        try {
            statement.executeUpdate();
            return cursorManager.open(database.generatedKeysSelector.selectGeneratedKeys(database.connection, statement,
                    database.accessorFactory, keys));
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
