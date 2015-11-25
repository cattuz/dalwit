package com.devexed.dalwit.jdbc;

import com.devexed.dalwit.*;
import com.devexed.dalwit.util.AbstractCloseableCloser;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

final class JdbcDatabase extends JdbcAbstractDatabase {

    JdbcDatabase(Connection connection, AccessorFactory<PreparedStatement, Integer, ResultSet, Integer, SQLException> accessorFactory,
                 JdbcGeneratedKeysSelector generatedKeysSelector) {
        super(Database.class, new AbstractCloseableCloser<Statement, JdbcStatement>(Connection.class, Database.class), connection, accessorFactory, generatedKeysSelector);
    }

    @Override
    public Transaction transact() {
        checkActive();
        JdbcRootTransaction transaction = new JdbcRootTransaction(this);
        openTransaction(transaction);
        return transaction;
    }

    @Override
    public void close() {
        statementManager.close();
        super.close();

        try {
            connection.close();
        } catch (SQLException e) {
            throw new DatabaseException(e);
        }
    }

}
