package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.Cursor;
import com.devexed.dalwit.DatabaseException;
import com.devexed.dalwit.InsertStatement;
import com.devexed.dalwit.Query;

import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

final class JdbcInsertStatement extends JdbcStatement implements InsertStatement {

    private final LinkedHashMap<String, Class<?>> keys;

    JdbcInsertStatement(JdbcAbstractDatabase database, Query query, Map<String, Class<?>> keys) {
        super(database, query);

        try {
            this.keys = new LinkedHashMap<>(keys);
            assignStatement(database.generatedKeysSelector.prepareInsertStatement(database.connection, query.createSql(), keys));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Cursor insert() {
        checkNotClosed();
        database.checkActive();

        try {
            statement.executeUpdate();
            return database.generatedKeysSelector.selectGeneratedKeys(database, statement, keys);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
