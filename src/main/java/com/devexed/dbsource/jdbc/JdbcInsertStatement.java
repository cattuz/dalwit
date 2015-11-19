package com.devexed.dbsource.jdbc;

import com.devexed.dbsource.*;

import java.sql.SQLException;
import java.util.Map;

final class JdbcInsertStatement extends JdbcStatement implements InsertStatement {

    public JdbcInsertStatement(JdbcAbstractDatabase database, Query query, Map<String, Class<?>> keys) {
        super(database, query, keys);
    }

    @Override
    public Cursor insert(Transaction transaction) {
        checkNotClosed();
        checkActiveTransaction(transaction);

        try {
            return statement.executeUpdate() > 0
                    ? database.generatedKeysSelector.selectGeneratedKeys(database, statement, database.accessorFactory, keys)
                    : EmptyCursor.of();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
