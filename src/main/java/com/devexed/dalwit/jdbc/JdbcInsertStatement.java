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

    public JdbcInsertStatement(JdbcAbstractDatabase database, Query query, Map<String, Class<?>> keys) {
        super(database, query);

        try {
            this.keys = new LinkedHashMap<String, Class<?>>(keys);
            setStatement(database.generatedKeysSelector.prepareInsertStatement(database.connection,
                    query.create(database, parameterIndexes, indexParameters), keys));
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public Cursor insert() {
        checkNotClosed();

        try {
            statement.executeUpdate();
            return database.generatedKeysSelector.selectGeneratedKeys(database.connection, statement,
                    database.accessorFactory, keys);
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
